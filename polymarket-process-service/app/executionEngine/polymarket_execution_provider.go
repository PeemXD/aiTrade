package executionengine

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/local/polymarket-process-service/pkg/config"
	"github.com/local/polymarket-process-service/pkg/httpclient"
	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
)

type PolymarketExecutionProvider struct {
	cfg    config.Config
	client *httpclient.Client
}

func NewPolymarketExecutionProvider(cfg config.Config) *PolymarketExecutionProvider {
	return &PolymarketExecutionProvider{cfg: cfg, client: httpclient.New(30 * time.Second)}
}

func (p *PolymarketExecutionProvider) PlaceOrder(ctx context.Context, order execmodel.OrderRequest) (execmodel.OrderResult, error) {
	if err := p.validateEnabled(order); err != nil {
		return execmodel.OrderResult{}, err
	}
	path := "/order"
	body := map[string]any{
		"market": order.MarketID, "outcome": order.Outcome, "side": strings.ToUpper(order.Side),
		"price": order.LimitPrice, "size": order.SizeUSD, "type": "GTC",
		"idempotency_key": order.IdempotencyKey,
	}
	headers := p.authHeaders(http.MethodPost, path)
	var raw map[string]any
	if err := p.client.PostJSON(ctx, strings.TrimRight(p.cfg.PolymarketCLOBBaseURL, "/")+path, headers, body, &raw); err != nil {
		return execmodel.OrderResult{}, err
	}
	return execmodel.OrderResult{Status: "pending", Message: "real order submitted"}, nil
}

func (p *PolymarketExecutionProvider) ClosePosition(context.Context, positionmodel.Position, string) (execmodel.OrderResult, error) {
	return execmodel.OrderResult{}, fmt.Errorf("real close position is intentionally not automated in MVP")
}

func (p *PolymarketExecutionProvider) validateEnabled(order execmodel.OrderRequest) error {
	if p.cfg.ExecutionMode != "real" || !p.cfg.EnableRealTrading {
		return fmt.Errorf("real trading disabled")
	}
	if p.cfg.RealTradingConfirmation != "I_UNDERSTAND_THIS_CAN_LOSE_MONEY" {
		return fmt.Errorf("real trading confirmation missing")
	}
	if p.cfg.PolyAPIKey == "" || p.cfg.PolyAPISecret == "" || p.cfg.PolyAPIPassphrase == "" || p.cfg.PolyPrivateKey == "" || p.cfg.PolyFunderAddress == "" {
		return fmt.Errorf("polymarket credentials incomplete")
	}
	key := strings.TrimPrefix(p.cfg.PolyPrivateKey, "0x")
	privateKey, err := crypto.HexToECDSA(key)
	if err != nil {
		return fmt.Errorf("invalid private key")
	}
	_ = crypto.PubkeyToAddress(privateKey.PublicKey)
	if err := validateLimitOrder(order); err != nil {
		return err
	}
	if order.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required for real trading")
	}
	return nil
}

func (p *PolymarketExecutionProvider) authHeaders(method, path string) map[string]string {
	ts := fmt.Sprint(time.Now().Unix())
	msg := ts + method + path
	mac := hmac.New(sha256.New, []byte(p.cfg.PolyAPISecret))
	_, _ = mac.Write([]byte(msg))
	signature := base64.StdEncoding.EncodeToString(mac.Sum(nil))
	return map[string]string{
		"POLY-API-KEY":        p.cfg.PolyAPIKey,
		"POLY-API-SIGNATURE":  signature,
		"POLY-API-TIMESTAMP":  ts,
		"POLY-API-PASSPHRASE": p.cfg.PolyAPIPassphrase,
		"POLY-FUNDER":         p.cfg.PolyFunderAddress,
	}
}
