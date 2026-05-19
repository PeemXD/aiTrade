package executionengine

import (
	"context"
	"fmt"
	"time"

	"github.com/local/polymarket-process-service/pkg/idgen"
	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	"github.com/local/polymarket-process-service/pkg/repository"
)

type PaperExecutionProvider struct {
	store repository.Store
}

func NewPaperExecutionProvider(store repository.Store) *PaperExecutionProvider {
	return &PaperExecutionProvider{store: store}
}

func (p *PaperExecutionProvider) PlaceOrder(ctx context.Context, order execmodel.OrderRequest) (execmodel.OrderResult, error) {
	market, err := p.store.GetMarket(ctx, order.MarketID)
	if err != nil {
		return execmodel.OrderResult{}, err
	}
	levels := market.OrderBook.Asks
	if order.Side == "sell" {
		levels = market.OrderBook.Bids
	}
	if len(levels) == 0 {
		price := order.LimitPrice
		if price == 0 {
			price = market.BestAsk
		}
		levels = []marketmodel.OrderBookLevel{{Price: price, Size: order.SizeUSD / price}}
	}
	fill := simulateFill(order.SizeUSD, order.LimitPrice, order.Side, levels)
	if fill.Quantity == 0 {
		return execmodel.OrderResult{Status: "rejected", Message: "no fillable liquidity"}, nil
	}
	now := time.Now().UTC()
	tradeID := idgen.New()
	status := "open"
	if fill.FilledSizeUSD < order.SizeUSD {
		status = "open"
	}
	trade := execmodel.Trade{
		ID: tradeID, Mode: "paper", MarketID: order.MarketID, Outcome: order.Outcome, Side: order.Side, Status: status,
		EntryPrice: fill.AveragePrice, SizeUSD: fill.FilledSizeUSD, Quantity: fill.Quantity, FeesUSD: fill.FeesUSD,
		SlippageUSD: fill.SlippageUSD, Reason: order.Reason, OpenedAt: &now, CreatedAt: now, UpdatedAt: now,
	}
	position := positionmodel.Position{
		ID: idgen.New(), MarketID: order.MarketID, Outcome: order.Outcome, Quantity: fill.Quantity,
		AvgEntryPrice: fill.AveragePrice, CurrentPrice: fill.AveragePrice, ExposureUSD: fill.FilledSizeUSD,
		Status: "open", OpenedAt: now, UpdatedAt: now,
	}
	if err := p.store.SaveTrade(ctx, trade); err != nil {
		return execmodel.OrderResult{}, err
	}
	if err := p.store.SavePosition(ctx, position); err != nil {
		return execmodel.OrderResult{}, err
	}
	_ = p.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "paper_trade_opened", EntityID: tradeID, Payload: map[string]any{"trade_id": tradeID, "market_id": order.MarketID, "entry_price": trade.EntryPrice, "size_usd": trade.SizeUSD}, CreatedAt: now})
	return execmodel.OrderResult{TradeID: tradeID, Status: status, AveragePrice: fill.AveragePrice, Quantity: fill.Quantity, FilledSizeUSD: fill.FilledSizeUSD, FeesUSD: fill.FeesUSD, SlippageUSD: fill.SlippageUSD}, nil
}

func (p *PaperExecutionProvider) ClosePosition(ctx context.Context, position positionmodel.Position, reason string) (execmodel.OrderResult, error) {
	market, err := p.store.GetMarket(ctx, position.MarketID)
	if err != nil {
		return execmodel.OrderResult{}, err
	}
	exitPrice := market.BestBid
	if exitPrice == 0 {
		exitPrice = position.CurrentPrice
	}
	now := time.Now().UTC()
	position.Status = "closed"
	position.CurrentPrice = exitPrice
	position.UnrealizedPnLUSD = 0
	position.UpdatedAt = now
	if err := p.store.SavePosition(ctx, position); err != nil {
		return execmodel.OrderResult{}, err
	}
	pnl := (exitPrice - position.AvgEntryPrice) * position.Quantity
	trade, found := p.matchingOpenTrade(ctx, position)
	if found {
		trade.Status = "closed"
		trade.ExitPrice = exitPrice
		trade.RealizedPnLUSD = pnl
		trade.UnrealizedPnLUSD = 0
		trade.ClosedAt = &now
		trade.UpdatedAt = now
	} else {
		trade = execmodel.Trade{
			ID: idgen.New(), Mode: "paper", MarketID: position.MarketID, Outcome: position.Outcome, Side: "sell", Status: "closed",
			EntryPrice: position.AvgEntryPrice, ExitPrice: exitPrice, SizeUSD: position.ExposureUSD, Quantity: position.Quantity,
			RealizedPnLUSD: pnl, Reason: reason, OpenedAt: &position.OpenedAt, ClosedAt: &now, CreatedAt: now, UpdatedAt: now,
		}
	}
	if err := p.store.SaveTrade(ctx, trade); err != nil {
		return execmodel.OrderResult{}, err
	}
	_ = p.store.SaveAudit(ctx, repository.AuditLog{ID: idgen.New(), Event: "paper_trade_closed", EntityID: trade.ID, Payload: map[string]any{"market_id": position.MarketID, "reason": reason, "realized_pnl_usd": pnl}, CreatedAt: now})
	return execmodel.OrderResult{TradeID: trade.ID, Status: "closed", AveragePrice: exitPrice, Quantity: position.Quantity, FilledSizeUSD: exitPrice * position.Quantity, Message: reason}, nil
}

func (p *PaperExecutionProvider) matchingOpenTrade(ctx context.Context, position positionmodel.Position) (execmodel.Trade, bool) {
	trades, err := p.store.ListTrades(ctx, 10000)
	if err != nil {
		return execmodel.Trade{}, false
	}
	for _, trade := range trades {
		if trade.Status != "open" || trade.MarketID != position.MarketID || trade.Outcome != position.Outcome {
			continue
		}
		if approxEqual(trade.Quantity, position.Quantity) || approxEqual(trade.SizeUSD, position.ExposureUSD) {
			return trade, true
		}
	}
	return execmodel.Trade{}, false
}

type fillResult struct {
	AveragePrice  float64
	Quantity      float64
	FilledSizeUSD float64
	FeesUSD       float64
	SlippageUSD   float64
}

func simulateFill(sizeUSD, limitPrice float64, side string, levels []marketmodel.OrderBookLevel) fillResult {
	remaining := sizeUSD
	quantity := 0.0
	filledUSD := 0.0
	expected := limitPrice
	for _, level := range levels {
		if level.Price <= 0 || level.Size <= 0 {
			continue
		}
		if side == "buy" && limitPrice > 0 && level.Price > limitPrice {
			continue
		}
		if side == "sell" && limitPrice > 0 && level.Price < limitPrice {
			continue
		}
		if expected == 0 {
			expected = level.Price
		}
		spend := min(remaining, level.Price*level.Size)
		shares := spend / level.Price
		quantity += shares
		filledUSD += spend
		remaining -= spend
		if remaining <= 0 {
			break
		}
	}
	if quantity == 0 {
		return fillResult{}
	}
	avg := filledUSD / quantity
	slippage := 0.0
	if expected > 0 {
		slippage = (avg - expected) * quantity
		if slippage < 0 {
			slippage = 0
		}
	}
	return fillResult{AveragePrice: avg, Quantity: quantity, FilledSizeUSD: filledUSD, FeesUSD: 0, SlippageUSD: slippage}
}

func validateLimitOrder(order execmodel.OrderRequest) error {
	if order.MarketID == "" || order.Outcome == "" || order.Side == "" {
		return fmt.Errorf("market_id, outcome, and side are required")
	}
	if order.LimitPrice <= 0 || order.LimitPrice >= 1 {
		return fmt.Errorf("limit_price must be between 0 and 1")
	}
	if order.SizeUSD <= 0 {
		return fmt.Errorf("size_usd must be positive")
	}
	return nil
}

func approxEqual(left, right float64) bool {
	diff := left - right
	if diff < 0 {
		diff = -diff
	}
	return diff < 0.000001
}
