package marketscanner

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExternalPolymarketMarketFetch(t *testing.T) {
	if os.Getenv("E2E_EXTERNAL") != "1" {
		t.Skip("set E2E_EXTERNAL=1 to run external integration tests")
	}
	client := NewPolymarketRESTClient("https://gamma-api.polymarket.com", "https://clob.polymarket.com", 20*time.Second)
	markets, err := client.FetchMarkets(context.Background(), 5)
	require.NoError(t, err)
	require.NotEmpty(t, markets)
}
