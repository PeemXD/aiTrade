package textmatch

import (
	"regexp"
	"strings"
)

var tokenRe = regexp.MustCompile(`[a-z0-9]+`)

var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "and": true, "or": true, "of": true, "to": true, "in": true,
	"on": true, "for": true, "will": true, "be": true, "is": true, "are": true, "by": true, "with": true,
	"before": true, "after": true, "from": true, "this": true, "that": true, "as": true, "at": true,
	"approved": true, "approve": true, "approval": true, "decision": true, "decide": true, "delays": true, "delay": true,
	"spot": true, "june": true, "july": true, "august": true, "september": true, "october": true, "november": true, "december": true,
}

var synonymEntities = map[string][]string{
	"bitcoin":  {"btc", "bitcoin"},
	"btc":      {"btc", "bitcoin"},
	"ethereum": {"eth", "ethereum", "ether"},
	"ether":    {"eth", "ethereum", "ether"},
	"eth":      {"eth", "ethereum", "ether"},
	"sec":      {"sec", "securities and exchange commission"},
	"fed":      {"fed", "federal reserve", "fomc"},
	"fomc":     {"fed", "federal reserve", "fomc"},
	"trump":    {"trump", "donald trump"},
	"etf":      {"etf", "exchange traded fund"},
}

func Keywords(text string) []string {
	text = strings.ToLower(text)
	seen := map[string]bool{}
	out := []string{}
	for _, tok := range tokenRe.FindAllString(text, -1) {
		if stopWords[tok] || len(tok) < 2 || isNumeric(tok) {
			continue
		}
		if canonical := canonicalToken(tok); canonical != "" {
			tok = canonical
		}
		if !seen[tok] {
			seen[tok] = true
			out = append(out, tok)
		}
	}
	return out
}

func Entities(text string) []string {
	keywords := Keywords(text)
	seen := map[string]bool{}
	out := []string{}
	for _, kw := range keywords {
		if vals, ok := synonymEntities[kw]; ok {
			for _, v := range vals {
				if !seen[v] {
					seen[v] = true
					out = append(out, v)
				}
			}
		}
	}
	return out
}

func isNumeric(tok string) bool {
	for _, r := range tok {
		if r < '0' || r > '9' {
			return false
		}
	}
	return tok != ""
}

func canonicalToken(tok string) string {
	switch tok {
	case "btc", "bitcoin":
		return "bitcoin"
	case "eth", "ether", "ethereum":
		return "ethereum"
	case "sec":
		return "sec"
	case "fed", "fomc":
		return "fed"
	case "trump", "donaldtrump":
		return "trump"
	case "etf":
		return "etf"
	}
	return tok
}
