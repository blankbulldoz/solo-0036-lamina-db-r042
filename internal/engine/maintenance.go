package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/index"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"time"
)

// maintenanceBudget bounds one unit of maintenance work between lock release
// points. Smaller budgets shorten the longest foreground pause at the cost of
// more maintenance rounds; the totals (how much work remains) do not change.
//
// The trade this makes explicit: the instance cannot hold a second copy of
// every index, so replacement is staged one index at a time and reclamation is
// applied in place. Foreground queries see either the old generation or the
// fully caught-up new one, never a partially built tree.
const maintenanceBudget = 96

type vacuumJob struct {
	Requested uint64
	Horizon   uint64
	Started   string
	Relations []string
	Relation  int
	Keys      []string
	Key       int
	Removed   int
	Phase     string
	Repair    []string
}

type rebuildJob struct {
	Name       string
	Definition catalog.Index
	Started    string
	Keys       []string
	Key        int
	AppliedSeq uint64
	Scanned    int
	Commits    int
	Phase      string
	generation *index.Generation
}

// MaintenanceStatus explains what maintenance is in progress right now. Both
// operations publish bounded progress so an operator can tell whether a pause
// comes from reclamation, a staged index switch, or a repair.
type MaintenanceStatus struct {
	Vacuum  *vacuumStatus  `json:"vacuum,omitempty"`
	Rebuild *rebuildStatus `json:"rebuild,omitempty"`
}
type vacuumStatus struct {
	Requested   uint64   `json:"requested"`
	Horizon     uint64   `json:"horizon"`
	Removed     int      `json:"removed"`
	KeysRemain  int      `json:"keys_remaining"`
	Phase       string   `json:"phase"`
	RepairQueue []string `json:"repair_queue,omitempty"`
}
type rebuildStatus struct {
	Index           string `json:"index"`
	Phase           string `json:"phase"`
	Scanned         int    `json:"scanned"`
	Total           int    `json:"total"`
	CommitsAbsorbed int    `json:"commits_absorbed"`
}

func (d *DB) maintenanceLocked() MaintenanceStatus {
	status := MaintenanceStatus{}
	if v := d.vacuum; v != nil {
		remain := 0
		for i := v.Relation; i < len(v.Relations); i++ {
			relation := v.Relations[i]
			if i == v.Relation && v.Keys != nil {
				remain += len(v.Keys) - v.Key
			} else {
				remain += len(d.store.Tables[relation])
			}
		}
		status.Vacuum = &vacuumStatus{Requested: v.Requested, Horizon: v.Horizon, Removed: v.Removed, KeysRemain: remain, Phase: v.Phase, RepairQueue: append([]string(nil), v.Repair...)}
	}
	if r := d.rebuild; r != nil {
		status.Rebuild = &rebuildStatus{Index: r.Name, Phase: r.Phase, Scanned: r.Scanned, Total: len(r.Keys), CommitsAbsorbed: r.Commits}
	}
	return status
}

// Vacuum advances the reclamation horizon and then removes every version that
// no retained snapshot can observe. Reclamation is incremental: index
// candidates are released version by version and dead entries are compacted in
// batches, while the write lock is released between bounded chunks so unrelated
// relations keep serving queries and commits. The operation returns only once
// the requested horizon has been fully applied, so its result remains an
// honest summary of what is reclaimable now.
func (d *DB) Vacuum(requested uint64) (versions.VacuumResult, error) {
	if !d.maintenanceMu.TryLock() {
		return versions.VacuumResult{}, fault("conflict", "another maintenance operation is in progress")
	}
	defer d.maintenanceMu.Unlock()
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return versions.VacuumResult{}, err
	}
	if requested > d.sequence || requested < d.horizon {
		d.mu.Unlock()
		return versions.VacuumResult{}, fault("invalid", "vacuum horizon outside retained interval")
	}
	horizon := requested
	for _, tx := range d.transactions {
		if tx.Snapshot < horizon {
			horizon = tx.Snapshot
		}
	}
	job := &vacuumJob{Requested: requested, Horizon: horizon, Started: time.Now().UTC().Format(time.RFC3339Nano), Relations: d.store.RelationNames(), Phase: "reclaim"}
	// The vacuum record is durable before any version disappears. Recovery
	// applies the horizon to the retained set, so an interrupted reclamation
	// can never resurrect a snapshot older than the returned horizon.
	if err := d.publish(journal.Record{Kind: "vacuum", Horizon: horizon}); err != nil {
		d.mu.Unlock()
		return versions.VacuumResult{}, err
	}
	d.vacuum = job
	d.mu.Unlock()
	for {
		done, err := d.vacuumChunk(job)
		if err != nil {
			d.mu.Lock()
			d.vacuum = nil
			d.mu.Unlock()
			return versions.VacuumResult{}, err
		}
		if done {
			break
		}
	}
	d.mu.Lock()
	d.vacuum = nil
	repair := append([]string(nil), job.Repair...)
	_, retained := d.store.Count()
	d.mu.Unlock()
	// An index that failed incremental release is not silently used: it is
	// replaced by a generation rebuilt from the retained chains before Vacuum
	// reports success.
	for _, name := range repair {
		if err := d.rebuildIndex(name); err != nil {
			return versions.VacuumResult{}, err
		}
	}
	return versions.VacuumResult{Horizon: horizon, Removed: job.Removed, Retained: retained, Pinned: horizon < requested}, nil
}

// vacuumChunk reclaims at most maintenanceBudget primary keys. It reacquires
// the write lock for the whole chunk, so a concurrent commit either lands
// before the chunk (and is not reclaimable at this horizon) or after the
// vacuum bookkeeping has advanced past the key.
func (d *DB) vacuumChunk(job *vacuumJob) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return false, err
	}
	budget := maintenanceBudget
	for budget > 0 && job.Relation < len(job.Relations) {
		relation := job.Relations[job.Relation]
		if job.Keys == nil {
			job.Keys = d.store.Keys(relation)
			job.Key = 0
		}
		if job.Key >= len(job.Keys) {
			d.indexes.Compact(64)
			job.Relation++
			job.Keys = nil
			job.Key = 0
			continue
		}
		key := job.Keys[job.Key]
		job.Key++
		budget--
		removed, _ := d.store.VacuumKey(relation, key, job.Horizon)
		job.Removed += len(removed)
		for _, version := range removed {
			if version.Deleted {
				continue
			}
			for name, definition := range d.catalog.Indexes {
				if definition.Relation != relation {
					continue
				}
				d.indexes.Release(name, version.Row[definition.Column], version.Key)
			}
		}
	}
	if job.Relation >= len(job.Relations) {
		d.indexes.Compact(1)
		job.Repair = d.indexes.Untrusted(d.catalog)
		return true, nil
	}
	return false, nil
}

// RebuildIndex replaces one index generation without rebuilding the others.
// The old generation keeps answering queries while the new one is scanned and
// then brought up to date from the journal under the same lock that installs
// it, so a foreground commit is never lost at the switch.
func (d *DB) RebuildIndex(name string) (map[string]any, error) {
	if !d.maintenanceMu.TryLock() {
		return nil, fault("conflict", "another maintenance operation is in progress")
	}
	defer d.maintenanceMu.Unlock()
	if err := d.rebuildIndex(name); err != nil {
		return nil, err
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rebuildSummary(name), nil
}

// rebuildIndex assumes maintenanceMu is held.
func (d *DB) rebuildIndex(name string) error {
	d.mu.Lock()
	if err := d.usable(); err != nil {
		d.mu.Unlock()
		return err
	}
	definition, ok := d.catalog.Indexes[name]
	if !ok {
		d.mu.Unlock()
		return fault("not_found", "unknown index %s", name)
	}
	job := &rebuildJob{
		Name:       name,
		Definition: definition,
		Started:    time.Now().UTC().Format(time.RFC3339Nano),
		Keys:       d.store.Keys(definition.Relation),
		AppliedSeq: d.sequence,
		Phase:      "scan",
	}
	job.generation = d.indexes.Begin(name, d.sequence)
	d.rebuild = job
	d.mu.Unlock()
	fail := func(err error) error {
		d.mu.Lock()
		d.indexes.Discard(name)
		d.rebuild = nil
		d.mu.Unlock()
		return err
	}
	for {
		done, err := d.rebuildChunk(job)
		if err != nil {
			return fail(err)
		}
		if done {
			break
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	// Catch-up and installation happen under one lock hold. Any commit that
	// ran between the scan and here is already in the journal and is replayed
	// into the replacement now; any commit after this point cannot start until
	// the replacement is installed. Either way no foreground write is lost at
	// the switch.
	job.Phase = "catchup"
	for _, r := range d.wal.Records {
		if r.Sequence <= job.AppliedSeq || r.Kind != "commit" {
			continue
		}
		for _, m := range r.Writes {
			if m.Deleted || m.Relation != job.Definition.Relation {
				continue
			}
			job.generation.Tree.Add(m.Row[job.Definition.Column], m.Key)
		}
		job.AppliedSeq = r.Sequence
		job.Commits++
	}
	if job.AppliedSeq < d.sequence {
		d.rebuild = nil
		d.indexes.Discard(name)
		return fault("unavailable", "rebuild could not catch up to sequence %d", d.sequence)
	}
	job.generation.Complete = true
	d.indexes.Install(name)
	d.rebuild = nil
	return nil
}

// rebuildChunk scans at most maintenanceBudget keys of the index relation.
func (d *DB) rebuildChunk(job *rebuildJob) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return false, err
	}
	budget := maintenanceBudget
	for budget > 0 && job.Key < len(job.Keys) {
		key := job.Keys[job.Key]
		job.Key++
		budget--
		for _, version := range d.store.Chain(job.Definition.Relation, key) {
			if version.Deleted {
				continue
			}
			job.generation.Tree.Add(version.Row[job.Definition.Column], key)
		}
		job.Scanned++
	}
	return job.Key >= len(job.Keys), nil
}

func (d *DB) rebuildSummary(name string) map[string]any {
	out := map[string]any{"index": name, "state": "trusted"}
	if generation, ok := d.indexes.Visible(name); ok {
		out["entries"] = generation.Tree.Size()
		out["built"] = generation.Built
	}
	return out
}

// Maintenance renders the trust and progress state. It is the answer to
// "which indexes are trustworthy after a failure or restart": restart rebuilds
// every generation, so an opened instance reports all of its indexes as
// trusted; a live incremental-release failure reports the affected index as
// untrusted until its replacement installs.
func (d *DB) maintenanceSnapshot(c *catalog.Catalog) (map[string]string, map[string]uint64, MaintenanceStatus) {
	return d.indexes.Report(c), d.indexes.Built(c), d.maintenanceLocked()
}
