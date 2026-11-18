package versions

import (
	"lamina/internal/datum"
	"sort"
)

type VacuumResult struct {
	Horizon  uint64 `json:"horizon"`
	Removed  int    `json:"removed"`
	Retained int    `json:"retained"`
	Pinned   bool   `json:"pinned"`
}

// Reclaimed describes one version that a vacuum step removed from a chain.
// Deleted versions are reported too so callers can account for them, but only
// non-deleted versions carry an indexed candidate that must be released.
type Reclaimed struct {
	Relation string
	Key      string
	Sequence uint64
	Row      datum.Row
	Deleted  bool
}

// Chain returns the retained versions for one primary key. Callers must not
// mutate the returned slice; VacuumKey replaces the chain rather than editing
// it in place so a previously returned chain stays valid.
func (s *Store) Chain(relation, key string) []Version {
	return s.Tables[relation][key]
}

func (s *Store) RelationNames() []string {
	out := make([]string, 0, len(s.Tables))
	for name := range s.Tables {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Keys copies the current primary keys of a relation. The copy is taken under
// the engine lock so maintenance can release that lock between chunks without
// carrying a live map iterator across concurrent commits. Sorting keeps
// maintenance progress and stats reproducible between runs.
func (s *Store) Keys(relation string) []string {
	out := make([]string, 0, len(s.Tables[relation]))
	for key := range s.Tables[relation] {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// VacuumKey reclaims the versions of one primary key that no snapshot in the
// retained interval can observe. It keeps the last version at or before the
// horizon plus every later version, and drops the whole chain when its final
// version is a tombstone at or before the horizon.
//
// The retained version may still be no older than the horizon because readers
// at the horizon observe it. Every removed version precedes it, so every
// allowed snapshot (at >= horizon) already resolves to it or something newer.
func (s *Store) VacuumKey(relation, key string, horizon uint64) (removed []Reclaimed, keysRemoved int) {
	rows := s.Tables[relation]
	chain, ok := rows[key]
	if !ok || len(chain) == 0 {
		return nil, 0
	}
	keep := 0
	for keep+1 < len(chain) && chain[keep+1].Sequence <= horizon {
		keep++
	}
	if chain[len(chain)-1].Sequence <= horizon && chain[len(chain)-1].Deleted {
		for _, v := range chain {
			removed = append(removed, Reclaimed{Relation: relation, Key: key, Sequence: v.Sequence, Row: datum.Clone(v.Row), Deleted: v.Deleted})
		}
		delete(rows, key)
		return removed, 1
	}
	for _, v := range chain[:keep] {
		removed = append(removed, Reclaimed{Relation: relation, Key: key, Sequence: v.Sequence, Row: datum.Clone(v.Row), Deleted: v.Deleted})
	}
	if keep > 0 {
		rows[key] = append([]Version(nil), chain[keep:]...)
	}
	return removed, 0
}

// Vacuum reclaims every relation in one call. Startup recovery uses it after
// replay, where no foreground work can interleave and the whole retained set
// is already in memory.
func (s *Store) Vacuum(horizon uint64) VacuumResult {
	result := VacuumResult{Horizon: horizon}
	for _, relation := range s.RelationNames() {
		for _, key := range s.Keys(relation) {
			removed, _ := s.VacuumKey(relation, key, horizon)
			result.Removed += len(removed)
		}
	}
	_, result.Retained = s.Count()
	return result
}
