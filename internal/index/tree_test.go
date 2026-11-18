package index

import (
	"lamina/internal/datum"
	"testing"
)

func intValue(n int64) datum.Value { return datum.Integer(n) }

func TestReleaseKeepsHistoricalCandidatesUntilLastReference(t *testing.T) {
	tree := NewTree()
	tree.Add(intValue(10), "k1") // overwritten version
	tree.Add(intValue(10), "k1") // retained version with the same value
	if tree.Size() != 1 {
		t.Fatalf("duplicate candidate not collapsed: %d", tree.Size())
	}
	if !tree.Release(intValue(10), "k1") {
		t.Fatalf("first release failed")
	}
	if got := tree.Candidates("=", intValue(10)); len(got) != 1 {
		t.Fatalf("candidate dropped while still referenced: %v", got)
	}
	if !tree.Release(intValue(10), "k1") {
		t.Fatalf("second release failed")
	}
	if got := tree.Candidates("=", intValue(10)); len(got) != 0 {
		t.Fatalf("dead candidate still returned: %v", got)
	}
	if tree.Size() != 0 || tree.Dead() != 1 {
		t.Fatalf("bookkeeping wrong: size=%d dead=%d", tree.Size(), tree.Dead())
	}
	if tree.Compact() != 1 || tree.Dead() != 0 {
		t.Fatalf("compaction did not drop dead entry")
	}
}

func TestReleaseMismatchReportsMissingCoverage(t *testing.T) {
	tree := NewTree()
	tree.Add(intValue(5), "k1")
	if tree.Release(intValue(6), "k1") {
		t.Fatalf("release of an unknown candidate must report missing coverage")
	}
	if tree.Release(intValue(5), "k1") != true {
		t.Fatalf("known candidate release failed")
	}
	if tree.Release(intValue(5), "k1") {
		t.Fatalf("double release must report missing coverage")
	}
}

func TestNullValuesAreNotIndexed(t *testing.T) {
	tree := NewTree()
	tree.Add(datum.Value{Kind: "null"}, "k1")
	if tree.Size() != 0 || !tree.Release(datum.Value{Kind: "null"}, "k1") {
		t.Fatalf("null handling wrong")
	}
}

func TestRangeCandidatesRespectOrder(t *testing.T) {
	tree := NewTree()
	for n := int64(1); n <= 5; n++ {
		tree.Add(intValue(n*10), "k"+string(rune('0'+n)))
	}
	got := tree.Candidates(">=", intValue(30))
	if len(got) != 3 || got[0] != "k3" || got[2] != "k5" {
		t.Fatalf("range candidates wrong: %v", got)
	}
}
