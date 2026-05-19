package aisignal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAIOutputRejectsEmptyReason(t *testing.T) {
	_, err := parseAIOutput(`{"related":true,"direction":"bullish","probability_delta":0.05,"confidence":0.7,"source_reliability":0.8,"priced_in_risk":"low","reason":""}`)

	require.Error(t, err)
	require.Contains(t, err.Error(), "reason")
}

func TestParseAIOutputRejectsUnrelatedNonNeutralOutput(t *testing.T) {
	_, err := parseAIOutput(`{"related":false,"direction":"bullish","probability_delta":0.01,"confidence":0.7,"source_reliability":0.8,"priced_in_risk":"low","reason":"not related"}`)

	require.Error(t, err)
}
