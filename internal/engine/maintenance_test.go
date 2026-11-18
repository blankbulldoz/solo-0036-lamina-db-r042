package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"testing"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seedRelation(t *testing.T, db *DB, name string) catalog.Relation {
	t.Helper()
	relation, err := db.CreateRelation(catalog.Relation{
		Name:    name,
		Primary: "id",
		Columns: []catalog.Column{
			{Name: "id", Type: "int"},
			{Name: "value", Type: "int", Nullable: true},
		},
	})
	if err != nil {
		t.Fatalf("create relation: %v", err)
	}
	return relation
}

func writeValue(t *testing.T, db *DB, relation string, id, value int64) uint64 {
	t.Helper()
	tx, err := db.Begin(false)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	row := datum.Row{"id": datum.Integer(id), "value": datum.Integer(value)}
	if err := db.Put(tx.ID, relation, row, false); err != nil {
		t.Fatalf("put: %v", err)
	}
	commit, err := db.Commit(tx.ID)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	return commit.Sequence
}

func buildIndex(t *testing.T, db *DB, name, relation string) {
	t.Helper()
	if _, err := db.CreateIndex(catalog.Index{Name: name, Relation: relation, Column: "value"}); err != nil {
		t.Fatalf("create index: %v", err)
	}
}

func TestVacuumReleasesOnlyUnreachableCandidates(t *testing.T) {
	db := openTestDB(t)
	seedRelation(t, db, "r")
	writeValue(t, db, "r", 1, 10)
	writeValue(t, db, "r", 1, 20)
	writeValue(t, db, "r", 1, 30)
	buildIndex(t, db, "by_value", "r")

	before := db.indexes.Entries(db.catalog)
	if before["by_value"] != 3 {
		t.Fatalf("history not indexed: %v", before)
	}
	head := db.sequence
	result, err := db.Vacuum(head)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if result.Horizon != head || result.Removed != 2 || result.Retained != 1 {
		t.Fatalf("unexpected vacuum result: %+v", result)
	}
	after := db.indexes.Entries(db.catalog)
	if after["by_value"] != 1 {
		t.Fatalf("obsolete candidates survived: %v", after)
	}
	if !db.indexes.Trusted("by_value") {
		t.Fatalf("index lost trust on a clean release")
	}
	// Retained version remains queryable through the index.
	rows, exists := db.store.Read("r", "int:1", db.sequence)
	if !exists || rows["value"].Int != 30 {
		t.Fatalf("retained version wrong: %v %v", rows, exists)
	}
}

func TestVacuumRepairsUntrustedIndexBeforeReturning(t *testing.T) {
	db := openTestDB(t)
	seedRelation(t, db, "r")
	writeValue(t, db, "r", 1, 10)
	writeValue(t, db, "r", 1, 20)
	buildIndex(t, db, "by_value", "r")

	// Simulate a candidate set that already lost coverage, e.g. a partial
	// repair after an interrupted manual step. Incremental release cannot
	// prove the remaining rows, so Vacuum must rebuild and re-trust it.
	db.mu.Lock()
	db.indexes.Release("by_value", datum.Integer(10), "int:1")
	db.indexes.Release("by_value", datum.Integer(10), "int:1")
	db.indexes.Release("by_value", datum.Integer(20), "int:1")
	db.indexes.Discard("by_value")
	db.mu.Unlock()
	if db.indexes.Trusted("by_value") {
		t.Fatalf("test setup did not untrust the index")
	}
	result, err := db.Vacuum(db.sequence)
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	if result.Retained != 1 {
		t.Fatalf("unexpected retained set: %+v", result)
	}
	if !db.indexes.Trusted("by_value") {
		t.Fatalf("vacuum returned with an untrusted index")
	}
	if got := db.indexes.Entries(db.catalog)["by_value"]; got != 1 {
		t.Fatalf("repair did not rebuild candidates: %d", got)
	}
}

func TestRebuildReplacesOnlyOneGeneration(t *testing.T) {
	db := openTestDB(t)
	seedRelation(t, db, "r")
	writeValue(t, db, "r", 1, 10)
	buildIndex(t, db, "by_value", "r")
	other, err := db.CreateRelation(catalog.Relation{Name: "s", Primary: "id", Columns: []catalog.Column{{Name: "id", Type: "int"}, {Name: "value", Type: "int"}}})
	if err != nil {
		t.Fatalf("create relation: %v", err)
	}
	if _, err = db.CreateIndex(catalog.Index{Name: "by_other", Relation: other.Name, Column: "value"}); err != nil {
		t.Fatalf("create index: %v", err)
	}
	first, ok := db.indexes.Visible("by_value")
	if !ok {
		t.Fatalf("missing generation")
	}
	if _, err = db.RebuildIndex("by_value"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	second, _ := db.indexes.Visible("by_value")
	if first == second {
		t.Fatalf("rebuild did not replace the generation")
	}
	if !first.Retired() {
		t.Fatalf("old generation was not retired")
	}
	otherGeneration, _ := db.indexes.Visible("by_other")
	if otherGeneration == nil || otherGeneration.Retired() {
		t.Fatalf("unrelated index generation was disturbed")
	}
	if _, ok = db.indexes.Visible("by_value"); !ok || db.indexes.Entries(db.catalog)["by_value"] != 1 {
		t.Fatalf("replacement generation missing candidates")
	}
}

func TestRebuildIsolatesPendingFromVisible(t *testing.T) {
	db := openTestDB(t)
	seedRelation(t, db, "r")
	writeValue(t, db, "r", 1, 10)
	buildIndex(t, db, "by_value", "r")
	visible, _ := db.indexes.Visible("by_value")

	db.mu.Lock()
	pending := db.indexes.Begin("by_value", db.sequence)
	db.mu.Unlock()
	if visible.Tree.Size() != 1 {
		t.Fatalf("visible generation changed by Begin")
	}
	if pending.Tree.Size() != 0 {
		t.Fatalf("pending generation unexpectedly populated")
	}
	db.mu.Lock()
	db.indexes.Discard("by_value")
	db.mu.Unlock()
	if current, _ := db.indexes.Visible("by_value"); current != visible {
		t.Fatalf("discarding a pending generation changed the visible one")
	}
}
