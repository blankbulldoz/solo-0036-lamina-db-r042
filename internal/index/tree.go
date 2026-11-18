package index

import (
	"lamina/internal/datum"
	"sort"
)

// Entry is one (value, primary key) candidate held by a secondary index.
// Refs counts the retained versions that still justify the candidate. A
// candidate is only dropped once every retained version referencing it has
// been reclaimed, so readers of any snapshot inside the retained interval keep
// finding their rows.
type Entry struct {
	Value datum.Value
	Key   string
	refs  int
}

// Tree is a typed ordered candidate set.
//
// Release never removes an entry from the ordered slice immediately. It
// decrements a reference count and marks the entry dead; Candidates and Size
// ignore dead entries, and Compact later filters them out in one linear pass.
// That keeps reclamation steps O(1) per removed version instead of a second
// full index build, and it lets foreground commits keep adding candidates
// while maintenance is in progress.
type Tree struct {
	entries []*Entry
	lookup  map[string]*Entry
	dead    int
}

func NewTree() *Tree { return &Tree{lookup: map[string]*Entry{}} }

func token(value datum.Value, key string) string { return value.Key() + "\x00" + key }

// Add registers one retained non-deleted version. Repeated values for the same
// key increase the reference count rather than duplicating the candidate.
func (t *Tree) Add(value datum.Value, key string) {
	if value.Null() {
		return
	}
	// Entries store no slice position because insertion shifts them; the
	// token map only needs to find the counter for an existing candidate.
	if entry, ok := t.lookup[token(value, key)]; ok && entry.refs > 0 {
		entry.refs++
		return
	}
	entry := &Entry{Value: value, Key: key, refs: 1}
	t.lookup[token(value, key)] = entry
	at := sort.Search(len(t.entries), func(i int) bool {
		current := t.entries[i]
		n := datum.Order(current.Value, value)
		return n > 0 || (n == 0 && current.Key >= key)
	})
	t.entries = append(t.entries, nil)
	copy(t.entries[at+1:], t.entries[at:])
	t.entries[at] = entry
}

// Release removes one reference for a reclaimed version. It returns false when
// the tree did not contain the candidate, which means the index can no longer
// be trusted to cover every retained row and must be rebuilt.
func (t *Tree) Release(value datum.Value, key string) bool {
	if value.Null() {
		return true
	}
	entry, ok := t.lookup[token(value, key)]
	if !ok || entry.refs <= 0 {
		return false
	}
	entry.refs--
	if entry.refs == 0 {
		delete(t.lookup, token(value, key))
		t.dead++
	}
	return true
}

// Compact drops dead entries in a single linear pass. It must run while the
// owning registry is protected by the engine lock.
func (t *Tree) Compact() int {
	if t.dead == 0 {
		return 0
	}
	live := t.entries[:0]
	for _, entry := range t.entries {
		if entry.refs > 0 {
			live = append(live, entry)
		}
	}
	removed := t.dead
	t.entries = live
	t.dead = 0
	return removed
}

func (t *Tree) Candidates(op string, value datum.Value) []string {
	if value.Null() {
		return nil
	}
	lo := sort.Search(len(t.entries), func(i int) bool { return datum.Order(t.entries[i].Value, value) >= 0 })
	hi := sort.Search(len(t.entries), func(i int) bool { return datum.Order(t.entries[i].Value, value) > 0 })
	begin, end := 0, len(t.entries)
	switch op {
	case "=":
		begin, end = lo, hi
	case ">":
		begin = hi
	case ">=":
		begin = lo
	case "<":
		end = lo
	case "<=":
		end = hi
	default:
		return nil
	}
	out := make([]string, 0, end-begin)
	seen := map[string]bool{}
	for _, e := range t.entries[begin:end] {
		if e.refs <= 0 || seen[e.Key] {
			continue
		}
		seen[e.Key] = true
		out = append(out, e.Key)
	}
	return out
}

// Size reports live candidates, not dead entries awaiting Compact.
func (t *Tree) Size() int { return len(t.entries) - t.dead }

// Dead reports entries whose references were all reclaimed but which have not
// yet been filtered by Compact.
func (t *Tree) Dead() int { return t.dead }
