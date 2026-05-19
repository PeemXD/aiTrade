package newsfetcher

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExternalGDELTFetch(t *testing.T) {
	if os.Getenv("E2E_EXTERNAL") != "1" {
		t.Skip("set E2E_EXTERNAL=1 to run external integration tests")
	}
	client := NewHTTPGDELTClient("https://api.gdeltproject.org/api/v2/doc/doc", 20*time.Second)
	articles, err := client.Fetch(context.Background(), "ethereum sourcelang:English", 5)
	require.NoError(t, err)
	require.NotEmpty(t, articles)
}
