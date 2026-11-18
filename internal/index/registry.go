package index

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/versions"
	"sort"
)

// Generation is one retained copy of a secondary index.
//
// The version chains define the ground truth: a generation is trusted only
// when it holds a candidate for every retained non-deleted version of its
// relation. Visible generations may keep historical candidates, which is a
// superset of any snapshot's visible rows; readers always recheck the version
// chain before returning a row.
//
// Once replaced, a generation is retired and never mutated again. Queries run
// under the engine lock, so one query never spans a switch; a reader that
// somehow outlived the switch would still hold a frozen tree rather than a
// tree whose entries were recycled underneath it.
type Generation struct {
	Tree     *Tree
	Built    uint64
	Complete bool
	retired  bool
}

// Registry owns the candidate sets keyed by index name.
//
// visible holds the query-visible generations. pending holds at most one
// replacement per index, produced by a staged rebuild. The replacement is
// filled by the scan and then by journal catch-up under the installing lock,
// so installing it cannot lose a foreground write. The registry never keeps
// two full copies of every index: either an index is maintained in place by
// reference-counted release, or one index at a time is being replaced.
type Registry struct {
	visible map[string]*Generation
	pending map[string]*Generation
	trusted map[string]bool
}

func NewRegistry() *Registry {
	return &Registry{visible: map[string]*Generation{}, pending: map[string]*Generation{}, trusted: map[string]bool{}}
}

func Build(c *catalog.Catalog, s *versions.Store) *Registry {
	r := NewRegistry()
	for name, definition := range c.Indexes {
		tree := buildTree(s, definition)
		r.visible[name] = &Generation{Tree: tree, Complete: true}
		r.trusted[name] = true
	}
	return r
}

func buildTree(s *versions.Store, definition catalog.Index) *Tree {
	tree := NewTree()
	for key, chain := range s.Tables[definition.Relation] {
		for _, v := range chain {
			if !v.Deleted {
				tree.Add(v.Row[definition.Column], key)
			}
		}
	}
	return tree
}

// AddVisible installs the tree of a newly created index without rebuilding or
// discarding the other index generations.
func (r *Registry) AddVisible(name string, tree *Tree, sequence uint64) {
	r.visible[name] = &Generation{Tree: tree, Built: sequence, Complete: true}
	r.trusted[name] = true
}

// Retired reports whether this generation was replaced. The registry drops it
// from Visible at installation, but any reader already holding it keeps the
// frozen tree.
func (g *Generation) Retired() bool { return g.retired }

// Visible returns the current generation. Callers hold the engine lock, so the
// returned tree cannot be replaced or compacted for the duration of a query.
func (r *Registry) Visible(name string) (*Generation, bool) {
	generation, ok := r.visible[name]
	return generation, ok
}

// SetBuilt stamps every generation built during recovery. A generation
// constructed from retained chains is complete at the recovery sequence.
func (r *Registry) SetBuilt(sequence uint64) {
	for _, generation := range r.visible {
		generation.Built = sequence
	}
}

func (r *Registry) Trusted(name string) bool { return r.trusted[name] }

// Apply folds a foreground commit into every visible tree of the affected
// relation. A pending replacement is deliberately not fed here: it is caught
// up from the journal under the same lock that installs it, so each committed
// version is counted exactly once instead of once per write path.
func (r *Registry) Apply(c *catalog.Catalog, writes []versions.Mutation) {
	for _, m := range writes {
		if m.Deleted {
			continue
		}
		for name, definition := range c.Indexes {
			if definition.Relation != m.Relation {
				continue
			}
			if generation, ok := r.visible[name]; ok {
				generation.Tree.Add(m.Row[definition.Column], m.Key)
			}
		}
	}
}

// Release retires one reclaimed version from an index. It reports false when
// the tree did not hold the candidate: the index can no longer be trusted to
// cover every retained row and the caller must rebuild it.
func (r *Registry) Release(name string, value datum.Value, key string) bool {
	generation, ok := r.visible[name]
	if !ok {
		return false
	}
	if !generation.Tree.Release(value, key) {
		r.trusted[name] = false
		return false
	}
	return true
}

// Begin stages a replacement generation. The visible generation keeps serving
// queries; the pending one is filled by the rebuild scan and catch-up.
func (r *Registry) Begin(name string, sequence uint64) *Generation {
	generation := &Generation{Tree: NewTree(), Built: sequence}
	r.pending[name] = generation
	return generation
}

// Install publishes a completed replacement and retires the old generation.
// The old tree is never mutated again; a reader that acquired it before the
// swap keeps a valid, frozen view until it calls Unacquire.
func (r *Registry) Install(name string) (*Generation, bool) {
	generation, ok := r.pending[name]
	if !ok {
		return nil, false
	}
	generation.Complete = true
	if old, exists := r.visible[name]; exists {
		old.retired = true
	}
	r.visible[name] = generation
	r.trusted[name] = generation.Complete
	delete(r.pending, name)
	return generation, true
}

// Discard abandons a pending replacement, leaving the visible tree untouched.
func (r *Registry) Discard(name string) { delete(r.pending, name) }

// Compact filters dead entries from trees with enough garbage to pay for the
// linear pass. Entries released to zero references are already ignored by
// Candidates, so compaction is a space/latency trade, not a correctness step.
func (r *Registry) Compact(minimum int) map[string]int {
	out := map[string]int{}
	for name, generation := range r.visible {
		tree := generation.Tree
		if tree.Dead() < minimum {
			continue
		}
		if removed := tree.Compact(); removed > 0 {
			out[name] = removed
		}
	}
	return out
}

// Report explains which indexes are currently trusted. "rebuilding" means the
// visible tree is still serving while a replacement is staged; "untrusted"
// means the planner must not use it.
func (r *Registry) Report(c *catalog.Catalog) map[string]string {
	out := map[string]string{}
	for name := range c.Indexes {
		switch {
		case r.pending[name] != nil:
			out[name] = "rebuilding"
		case r.trusted[name]:
			out[name] = "trusted"
		default:
			out[name] = "untrusted"
		}
	}
	return out
}

// Untrusted lists indexes that need a repair rebuild, sorted for stable output.
func (r *Registry) Untrusted(c *catalog.Catalog) []string {
	var out []string
	for name := range c.Indexes {
		if !r.trusted[name] && r.pending[name] == nil {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Entries reports live candidate counts for every visible tree.
func (r *Registry) Entries(c *catalog.Catalog) map[string]int {
	out := map[string]int{}
	for name := range c.Indexes {
		if generation, ok := r.visible[name]; ok {
			out[name] = generation.Tree.Size()
		}
	}
	return out
}

// Built reports the sequence at which each visible generation was constructed.
func (r *Registry) Built(c *catalog.Catalog) map[string]uint64 {
	out := map[string]uint64{}
	for name := range c.Indexes {
		if generation, ok := r.visible[name]; ok {
			out[name] = generation.Built
		}
	}
	return out
}
