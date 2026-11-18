package query

import (
	"lamina/internal/catalog"
	"lamina/internal/index"
	"sort"
)

// Choose selects an index access path only from generations the registry still
// trusts. A generation under repair is skipped and the caller falls back to a
// scan: an untrusted candidate set can omit a row, while a scan cannot.
func Choose(c *catalog.Catalog, registry *index.Registry, q Request, at uint64, total int) (Plan, []string) {
	plan := Plan{Access: "scan", Candidates: total, Snapshot: at, Residuals: len(q.Where)}
	if q.ForceScan || registry == nil {
		return plan, nil
	}
	names := make([]string, 0, len(c.Indexes))
	for name := range c.Indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	var chosen []string
	for _, name := range names {
		definition := c.Indexes[name]
		if definition.Relation != q.Relation || definition.Created > at || !registry.Trusted(name) {
			continue
		}
		generation, ok := registry.Visible(name)
		if !ok {
			continue
		}
		for _, p := range q.Where {
			if p.Column != definition.Column || p.Value.Null() {
				continue
			}
			switch p.Op {
			case "=", "<", ">", "<=", ">=":
			default:
				continue
			}
			candidates := generation.Tree.Candidates(p.Op, p.Value)
			if len(candidates) < plan.Candidates || plan.Access == "scan" && len(candidates) == 0 {
				plan.Access = "index"
				plan.Index = name
				plan.Candidates = len(candidates)
				chosen = candidates
			}
		}
	}
	return plan, chosen
}
