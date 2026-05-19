package main

import "testing"

func TestSeenTrackerEvictsOldestArticle(t *testing.T) {
	seen := newSeenTracker(2)

	seen.Add("a")
	seen.Add("b")
	seen.Add("c")

	if seen.Has("a") {
		t.Fatal("oldest article was not evicted")
	}
	if !seen.Has("b") || !seen.Has("c") {
		t.Fatal("recent articles should remain tracked")
	}
}
