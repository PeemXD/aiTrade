package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	aimodel "github.com/local/polymarket-process-service/pkg/model/ai"
	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	matchmodel "github.com/local/polymarket-process-service/pkg/model/match"
	newsmodel "github.com/local/polymarket-process-service/pkg/model/news"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
	riskmodel "github.com/local/polymarket-process-service/pkg/model/risk"
)

type AuditLog struct {
	ID        string         `json:"id"`
	Event     string         `json:"event"`
	EntityID  string         `json:"entity_id,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

const (
	maxAISignalList    = 1000
	maxMemoryAuditLogs = 5000
)

type Store interface {
	SaveMarkets(context.Context, []marketmodel.Market) error
	ListMarkets(context.Context) ([]marketmodel.Market, error)
	GetMarket(context.Context, string) (marketmodel.Market, error)
	SaveLiveMarketState(context.Context, marketmodel.LiveMarketState) error
	GetLiveMarketState(context.Context, string) (marketmodel.LiveMarketState, error)

	SaveNewsArticles(context.Context, []newsmodel.NewsArticle) error
	ListNewsArticles(context.Context, int) ([]newsmodel.NewsArticle, error)
	GetNewsArticle(context.Context, string) (newsmodel.NewsArticle, error)

	SaveMatch(context.Context, matchmodel.NewsMarketMatch) error
	ListMatchesForNews(context.Context, string) ([]matchmodel.NewsMarketMatch, error)

	SaveAISignal(context.Context, aimodel.AISignal) error
	ListAISignals(context.Context, string, string) ([]aimodel.AISignal, error)
	LatestAISignal(context.Context, string, string) (aimodel.AISignal, error)

	SaveProbabilityDecision(context.Context, probmodel.ProbabilityDecision) error
	GetProbabilityDecision(context.Context, string) (probmodel.ProbabilityDecision, error)
	ListProbabilityDecisions(context.Context, int) ([]probmodel.ProbabilityDecision, error)

	SaveRiskDecision(context.Context, riskmodel.RiskDecision) error
	GetRiskDecision(context.Context, string) (riskmodel.RiskDecision, error)
	ListRiskDecisions(context.Context, int) ([]riskmodel.RiskDecision, error)

	SaveTrade(context.Context, execmodel.Trade) error
	GetTrade(context.Context, string) (execmodel.Trade, error)
	ListTrades(context.Context, int) ([]execmodel.Trade, error)
	SavePosition(context.Context, positionmodel.Position) error
	GetPosition(context.Context, string) (positionmodel.Position, error)
	ListPositions(context.Context, string) ([]positionmodel.Position, error)

	Portfolio(context.Context, float64) (riskmodel.PortfolioState, error)
	SaveAudit(context.Context, AuditLog) error
	ListAudit(context.Context, int) ([]AuditLog, error)
	Close()
}

type MemoryStore struct {
	mu         sync.RWMutex
	markets    map[string]marketmodel.Market
	liveStates map[string]marketmodel.LiveMarketState
	articles   map[string]newsmodel.NewsArticle
	matches    map[string]matchmodel.NewsMarketMatch
	signals    map[string]aimodel.AISignal
	probs      map[string]probmodel.ProbabilityDecision
	risks      map[string]riskmodel.RiskDecision
	trades     map[string]execmodel.Trade
	positions  map[string]positionmodel.Position
	audit      []AuditLog
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		markets: map[string]marketmodel.Market{}, liveStates: map[string]marketmodel.LiveMarketState{},
		articles: map[string]newsmodel.NewsArticle{}, matches: map[string]matchmodel.NewsMarketMatch{},
		signals: map[string]aimodel.AISignal{}, probs: map[string]probmodel.ProbabilityDecision{},
		risks: map[string]riskmodel.RiskDecision{}, trades: map[string]execmodel.Trade{},
		positions: map[string]positionmodel.Position{},
	}
}

func (s *MemoryStore) SaveMarkets(_ context.Context, markets []marketmodel.Market) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range markets {
		s.markets[m.ID] = m
	}
	return nil
}

func (s *MemoryStore) ListMarkets(context.Context) ([]marketmodel.Market, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]marketmodel.Market, 0, len(s.markets))
	for _, v := range s.markets {
		out = append(out, v)
	}
	return out, nil
}

func (s *MemoryStore) GetMarket(_ context.Context, id string) (marketmodel.Market, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.markets[id]; ok {
		return v, nil
	}
	return marketmodel.Market{}, pgx.ErrNoRows
}

func (s *MemoryStore) SaveLiveMarketState(_ context.Context, st marketmodel.LiveMarketState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.liveStates[st.MarketID] = st
	return nil
}

func (s *MemoryStore) GetLiveMarketState(_ context.Context, marketID string) (marketmodel.LiveMarketState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.liveStates[marketID]; ok {
		return v, nil
	}
	return marketmodel.LiveMarketState{}, pgx.ErrNoRows
}

func (s *MemoryStore) SaveNewsArticles(_ context.Context, articles []newsmodel.NewsArticle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, a := range articles {
		exists := false
		for _, old := range s.articles {
			if old.Hash == a.Hash {
				exists = true
				break
			}
		}
		if !exists {
			s.articles[a.ID] = a
		}
	}
	return nil
}

func (s *MemoryStore) ListNewsArticles(_ context.Context, limit int) ([]newsmodel.NewsArticle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]newsmodel.NewsArticle, 0, len(s.articles))
	for _, v := range s.articles {
		out = append(out, v)
	}
	return capLimit(out, limit), nil
}

func (s *MemoryStore) GetNewsArticle(_ context.Context, id string) (newsmodel.NewsArticle, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.articles[id]; ok {
		return v, nil
	}
	return newsmodel.NewsArticle{}, pgx.ErrNoRows
}

func (s *MemoryStore) SaveMatch(_ context.Context, m matchmodel.NewsMarketMatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.matches[m.ID] = m
	return nil
}

func (s *MemoryStore) ListMatchesForNews(_ context.Context, newsID string) ([]matchmodel.NewsMarketMatch, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []matchmodel.NewsMarketMatch{}
	for _, v := range s.matches {
		if v.NewsID == newsID {
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *MemoryStore) SaveAISignal(_ context.Context, sig aimodel.AISignal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.signals[sig.ID] = sig
	return nil
}

func (s *MemoryStore) ListAISignals(_ context.Context, marketID, newsID string) ([]aimodel.AISignal, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []aimodel.AISignal{}
	for _, v := range s.signals {
		if (marketID == "" || v.MarketID == marketID) && (newsID == "" || v.NewsID == newsID) {
			out = append(out, v)
		}
	}
	return capLimit(out, maxAISignalList), nil
}

func (s *MemoryStore) LatestAISignal(ctx context.Context, marketID, newsID string) (aimodel.AISignal, error) {
	items, _ := s.ListAISignals(ctx, marketID, newsID)
	if len(items) == 0 {
		return aimodel.AISignal{}, pgx.ErrNoRows
	}
	latest := items[0]
	for _, item := range items[1:] {
		if item.CreatedAt.After(latest.CreatedAt) {
			latest = item
		}
	}
	return latest, nil
}

func (s *MemoryStore) SaveProbabilityDecision(_ context.Context, d probmodel.ProbabilityDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.probs[d.ID] = d
	return nil
}

func (s *MemoryStore) GetProbabilityDecision(_ context.Context, id string) (probmodel.ProbabilityDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.probs[id]; ok {
		return v, nil
	}
	return probmodel.ProbabilityDecision{}, pgx.ErrNoRows
}

func (s *MemoryStore) ListProbabilityDecisions(_ context.Context, limit int) ([]probmodel.ProbabilityDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]probmodel.ProbabilityDecision, 0, len(s.probs))
	for _, v := range s.probs {
		out = append(out, v)
	}
	return capLimit(out, limit), nil
}

func (s *MemoryStore) SaveRiskDecision(_ context.Context, d riskmodel.RiskDecision) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.risks[d.ID] = d
	return nil
}

func (s *MemoryStore) GetRiskDecision(_ context.Context, id string) (riskmodel.RiskDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.risks[id]; ok {
		return v, nil
	}
	return riskmodel.RiskDecision{}, pgx.ErrNoRows
}

func (s *MemoryStore) ListRiskDecisions(_ context.Context, limit int) ([]riskmodel.RiskDecision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]riskmodel.RiskDecision, 0, len(s.risks))
	for _, v := range s.risks {
		out = append(out, v)
	}
	return capLimit(out, limit), nil
}

func (s *MemoryStore) SaveTrade(_ context.Context, t execmodel.Trade) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.trades[t.ID] = t
	return nil
}

func (s *MemoryStore) GetTrade(_ context.Context, id string) (execmodel.Trade, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.trades[id]; ok {
		return v, nil
	}
	return execmodel.Trade{}, pgx.ErrNoRows
}

func (s *MemoryStore) ListTrades(_ context.Context, limit int) ([]execmodel.Trade, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]execmodel.Trade, 0, len(s.trades))
	for _, v := range s.trades {
		out = append(out, v)
	}
	return capLimit(out, limit), nil
}

func (s *MemoryStore) SavePosition(_ context.Context, p positionmodel.Position) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.positions[p.ID] = p
	return nil
}

func (s *MemoryStore) GetPosition(_ context.Context, id string) (positionmodel.Position, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if v, ok := s.positions[id]; ok {
		return v, nil
	}
	return positionmodel.Position{}, pgx.ErrNoRows
}

func (s *MemoryStore) ListPositions(_ context.Context, status string) ([]positionmodel.Position, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []positionmodel.Position{}
	for _, v := range s.positions {
		if status == "" || v.Status == status {
			out = append(out, v)
		}
	}
	return out, nil
}

func (s *MemoryStore) Portfolio(_ context.Context, startingCash float64) (riskmodel.PortfolioState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state := riskmodel.PortfolioState{CashUSD: startingCash, PortfolioValueUSD: startingCash}
	for _, p := range s.positions {
		if p.Status == "open" {
			state.ExposureUSD += p.ExposureUSD
			state.OpenPnLUSD += p.UnrealizedPnLUSD
			state.OpenPositions++
		}
	}
	for _, t := range s.trades {
		if t.Status == "open" {
			state.CashUSD -= t.SizeUSD + t.FeesUSD
		}
		state.RealizedPnLUSD += t.RealizedPnLUSD
	}
	state.PortfolioValueUSD = state.CashUSD + state.ExposureUSD + state.OpenPnLUSD
	state.DailyPnLUSD = state.RealizedPnLUSD + state.OpenPnLUSD
	if startingCash > 0 {
		state.DailyPnLPct = state.DailyPnLUSD / startingCash
	}
	return state, nil
}

func (s *MemoryStore) SaveAudit(_ context.Context, a AuditLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit = append(s.audit, a)
	if len(s.audit) > maxMemoryAuditLogs {
		s.audit = append([]AuditLog(nil), s.audit[len(s.audit)-maxMemoryAuditLogs:]...)
	}
	return nil
}

func (s *MemoryStore) ListAudit(_ context.Context, limit int) ([]AuditLog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return capLimit(s.audit, limit), nil
}

func (s *MemoryStore) Close() {}

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(ctx context.Context, databaseURL string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

func (s *PostgresStore) Close() { s.pool.Close() }

func (s *PostgresStore) SaveMarkets(ctx context.Context, markets []marketmodel.Market) error {
	for _, m := range markets {
		book := mustJSON(m.OrderBook)
		_, err := s.pool.Exec(ctx, `insert into markets
			(id, condition_id, question, slug, category, active, closed, end_time, volume, liquidity, yes_token_id, no_token_id, yes_price, no_price, best_bid, best_ask, spread, orderbook, created_at, updated_at)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
			on conflict (id) do update set condition_id=excluded.condition_id, question=excluded.question, slug=excluded.slug,
			category=excluded.category, active=excluded.active, closed=excluded.closed, end_time=excluded.end_time,
			volume=excluded.volume, liquidity=excluded.liquidity, yes_token_id=excluded.yes_token_id, no_token_id=excluded.no_token_id,
			yes_price=excluded.yes_price, no_price=excluded.no_price, best_bid=excluded.best_bid, best_ask=excluded.best_ask,
			spread=excluded.spread, orderbook=excluded.orderbook, updated_at=excluded.updated_at`,
			m.ID, m.ConditionID, m.Question, m.Slug, m.Category, m.Active, m.Closed, m.EndTime, m.Volume, m.Liquidity,
			m.YesTokenID, m.NoTokenID, m.YesPrice, m.NoPrice, m.BestBid, m.BestAsk, m.Spread, book, m.CreatedAt, m.UpdatedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) ListMarkets(ctx context.Context) ([]marketmodel.Market, error) {
	rows, err := s.pool.Query(ctx, `select id, condition_id, question, slug, category, active, closed, end_time, volume, liquidity, yes_token_id, no_token_id, yes_price, no_price, best_bid, best_ask, spread, orderbook, created_at, updated_at from markets order by updated_at desc`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMarkets(rows)
}

func (s *PostgresStore) GetMarket(ctx context.Context, id string) (marketmodel.Market, error) {
	rows, err := s.pool.Query(ctx, `select id, condition_id, question, slug, category, active, closed, end_time, volume, liquidity, yes_token_id, no_token_id, yes_price, no_price, best_bid, best_ask, spread, orderbook, created_at, updated_at from markets where id=$1`, id)
	if err != nil {
		return marketmodel.Market{}, err
	}
	defer rows.Close()
	items, err := scanMarkets(rows)
	if err != nil {
		return marketmodel.Market{}, err
	}
	if len(items) == 0 {
		return marketmodel.Market{}, pgx.ErrNoRows
	}
	return items[0], nil
}

func (s *PostgresStore) SaveLiveMarketState(ctx context.Context, st marketmodel.LiveMarketState) error {
	_, err := s.pool.Exec(ctx, `insert into live_market_states (market_id, asset_id, best_bid, best_ask, last_price, mid_price, spread, orderbook_bids, orderbook_asks, updated_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		on conflict (market_id) do update set asset_id=excluded.asset_id, best_bid=excluded.best_bid, best_ask=excluded.best_ask, last_price=excluded.last_price,
		mid_price=excluded.mid_price, spread=excluded.spread, orderbook_bids=excluded.orderbook_bids, orderbook_asks=excluded.orderbook_asks, updated_at=excluded.updated_at`,
		st.MarketID, st.AssetID, st.BestBid, st.BestAsk, st.LastPrice, st.MidPrice, st.Spread, mustJSON(st.OrderBookBids), mustJSON(st.OrderBookAsks), st.UpdatedAt)
	return err
}

func (s *PostgresStore) GetLiveMarketState(ctx context.Context, marketID string) (marketmodel.LiveMarketState, error) {
	var st marketmodel.LiveMarketState
	var bids, asks []byte
	err := s.pool.QueryRow(ctx, `select market_id, asset_id, best_bid, best_ask, last_price, mid_price, spread, orderbook_bids, orderbook_asks, updated_at from live_market_states where market_id=$1`, marketID).
		Scan(&st.MarketID, &st.AssetID, &st.BestBid, &st.BestAsk, &st.LastPrice, &st.MidPrice, &st.Spread, &bids, &asks, &st.UpdatedAt)
	if err != nil {
		return st, err
	}
	_ = json.Unmarshal(bids, &st.OrderBookBids)
	_ = json.Unmarshal(asks, &st.OrderBookAsks)
	return st, nil
}

func (s *PostgresStore) SaveNewsArticles(ctx context.Context, articles []newsmodel.NewsArticle) error {
	for _, a := range articles {
		_, err := s.pool.Exec(ctx, `insert into news_articles (id, source, title, url, content, summary, published_at, fetched_at, hash, entities, keywords)
			values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) on conflict (hash) do nothing`,
			a.ID, a.Source, a.Title, a.URL, a.Content, a.Summary, a.PublishedAt, a.FetchedAt, a.Hash, mustJSON(a.Entities), mustJSON(a.Keywords))
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) ListNewsArticles(ctx context.Context, limit int) ([]newsmodel.NewsArticle, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `select id, source, title, url, content, summary, published_at, fetched_at, hash, entities, keywords from news_articles order by published_at desc nulls last, fetched_at desc limit $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanArticles(rows)
}

func (s *PostgresStore) GetNewsArticle(ctx context.Context, id string) (newsmodel.NewsArticle, error) {
	rows, err := s.pool.Query(ctx, `select id, source, title, url, content, summary, published_at, fetched_at, hash, entities, keywords from news_articles where id=$1`, id)
	if err != nil {
		return newsmodel.NewsArticle{}, err
	}
	defer rows.Close()
	items, err := scanArticles(rows)
	if err != nil {
		return newsmodel.NewsArticle{}, err
	}
	if len(items) == 0 {
		return newsmodel.NewsArticle{}, pgx.ErrNoRows
	}
	return items[0], nil
}

func (s *PostgresStore) SaveMatch(ctx context.Context, m matchmodel.NewsMarketMatch) error {
	_, err := s.pool.Exec(ctx, `insert into news_market_matches (id, news_id, market_id, keyword_score, entity_score, embedding_score, final_score, reason, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9) on conflict (id) do nothing`,
		m.ID, m.NewsID, m.MarketID, m.KeywordScore, m.EntityScore, m.EmbeddingScore, m.FinalScore, m.Reason, m.CreatedAt)
	return err
}

func (s *PostgresStore) ListMatchesForNews(ctx context.Context, newsID string) ([]matchmodel.NewsMarketMatch, error) {
	rows, err := s.pool.Query(ctx, `select id, news_id, market_id, keyword_score, entity_score, embedding_score, final_score, reason, created_at from news_market_matches where news_id=$1 order by final_score desc`, newsID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []matchmodel.NewsMarketMatch{}
	for rows.Next() {
		var m matchmodel.NewsMarketMatch
		if err := rows.Scan(&m.ID, &m.NewsID, &m.MarketID, &m.KeywordScore, &m.EntityScore, &m.EmbeddingScore, &m.FinalScore, &m.Reason, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *PostgresStore) SaveAISignal(ctx context.Context, sig aimodel.AISignal) error {
	_, err := s.pool.Exec(ctx, `insert into ai_signals (id, market_id, news_id, related, direction, probability_delta, confidence, source_reliability, priced_in_risk, reasoning, raw_response, disabled, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		sig.ID, sig.MarketID, sig.NewsID, sig.Related, sig.Direction, sig.ProbabilityDelta, sig.Confidence, sig.SourceReliability, sig.PricedInRisk, sig.Reasoning, sig.RawResponse, sig.Disabled, sig.CreatedAt)
	return err
}

func (s *PostgresStore) ListAISignals(ctx context.Context, marketID, newsID string) ([]aimodel.AISignal, error) {
	rows, err := s.pool.Query(ctx, `select id, market_id, news_id, related, direction, probability_delta, confidence, source_reliability, priced_in_risk, reasoning, raw_response, disabled, created_at
		from ai_signals where ($1='' or market_id=$1) and ($2='' or news_id=$2) order by created_at desc limit $3`, marketID, newsID, maxAISignalList)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []aimodel.AISignal{}
	for rows.Next() {
		var sig aimodel.AISignal
		if err := rows.Scan(&sig.ID, &sig.MarketID, &sig.NewsID, &sig.Related, &sig.Direction, &sig.ProbabilityDelta, &sig.Confidence, &sig.SourceReliability, &sig.PricedInRisk, &sig.Reasoning, &sig.RawResponse, &sig.Disabled, &sig.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, sig)
	}
	return out, rows.Err()
}

func (s *PostgresStore) LatestAISignal(ctx context.Context, marketID, newsID string) (aimodel.AISignal, error) {
	items, err := s.ListAISignals(ctx, marketID, newsID)
	if err != nil {
		return aimodel.AISignal{}, err
	}
	if len(items) == 0 {
		return aimodel.AISignal{}, pgx.ErrNoRows
	}
	return items[0], nil
}

func (s *PostgresStore) SaveProbabilityDecision(ctx context.Context, d probmodel.ProbabilityDecision) error {
	_, err := s.pool.Exec(ctx, `insert into probability_decisions (id, market_id, news_id, market_probability, executable_price, our_probability, edge, confidence, components_json, decision, reason, side, outcome, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		d.ID, d.MarketID, d.NewsID, d.MarketProbability, d.ExecutablePrice, d.OurProbability, d.Edge, d.Confidence, mustJSON(d.Components), d.Decision, d.Reason, d.Side, d.Outcome, d.CreatedAt)
	return err
}

func (s *PostgresStore) GetProbabilityDecision(ctx context.Context, id string) (probmodel.ProbabilityDecision, error) {
	rows, err := s.pool.Query(ctx, `select id, market_id, news_id, market_probability, executable_price, our_probability, edge, confidence, components_json, decision, reason, side, outcome, created_at from probability_decisions where id=$1`, id)
	if err != nil {
		return probmodel.ProbabilityDecision{}, err
	}
	defer rows.Close()
	items, err := scanProbability(rows)
	if err != nil {
		return probmodel.ProbabilityDecision{}, err
	}
	if len(items) == 0 {
		return probmodel.ProbabilityDecision{}, pgx.ErrNoRows
	}
	return items[0], nil
}

func (s *PostgresStore) ListProbabilityDecisions(ctx context.Context, limit int) ([]probmodel.ProbabilityDecision, error) {
	rows, err := s.pool.Query(ctx, `select id, market_id, news_id, market_probability, executable_price, our_probability, edge, confidence, components_json, decision, reason, side, outcome, created_at from probability_decisions order by created_at desc limit $1`, defaultLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProbability(rows)
}

func (s *PostgresStore) SaveRiskDecision(ctx context.Context, d riskmodel.RiskDecision) error {
	_, err := s.pool.Exec(ctx, `insert into risk_decisions (id, market_id, probability_decision_id, approved, position_size_usd, max_loss_usd, checks_json, reject_reason, reason, created_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		d.ID, d.MarketID, d.ProbabilityDecisionID, d.Approved, d.PositionSizeUSD, d.MaxLossUSD, mustJSON(d.Checks), d.RejectReason, d.Reason, d.CreatedAt)
	return err
}

func (s *PostgresStore) GetRiskDecision(ctx context.Context, id string) (riskmodel.RiskDecision, error) {
	rows, err := s.pool.Query(ctx, `select id, market_id, probability_decision_id, approved, position_size_usd, max_loss_usd, checks_json, reject_reason, reason, created_at from risk_decisions where id=$1`, id)
	if err != nil {
		return riskmodel.RiskDecision{}, err
	}
	defer rows.Close()
	items, err := scanRisk(rows)
	if err != nil {
		return riskmodel.RiskDecision{}, err
	}
	if len(items) == 0 {
		return riskmodel.RiskDecision{}, pgx.ErrNoRows
	}
	return items[0], nil
}

func (s *PostgresStore) ListRiskDecisions(ctx context.Context, limit int) ([]riskmodel.RiskDecision, error) {
	rows, err := s.pool.Query(ctx, `select id, market_id, probability_decision_id, approved, position_size_usd, max_loss_usd, checks_json, reject_reason, reason, created_at from risk_decisions order by created_at desc limit $1`, defaultLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRisk(rows)
}

func (s *PostgresStore) SaveTrade(ctx context.Context, t execmodel.Trade) error {
	_, err := s.pool.Exec(ctx, `insert into trades (id, mode, market_id, outcome, side, status, entry_price, exit_price, size_usd, quantity, fees_usd, slippage_usd, realized_pnl_usd, unrealized_pnl_usd, reason, opened_at, closed_at, created_at, updated_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
		on conflict (id) do update set status=excluded.status, exit_price=excluded.exit_price, realized_pnl_usd=excluded.realized_pnl_usd, unrealized_pnl_usd=excluded.unrealized_pnl_usd, closed_at=excluded.closed_at, updated_at=excluded.updated_at`,
		t.ID, t.Mode, t.MarketID, t.Outcome, t.Side, t.Status, t.EntryPrice, t.ExitPrice, t.SizeUSD, t.Quantity, t.FeesUSD, t.SlippageUSD, t.RealizedPnLUSD, t.UnrealizedPnLUSD, t.Reason, t.OpenedAt, t.ClosedAt, t.CreatedAt, t.UpdatedAt)
	return err
}

func (s *PostgresStore) GetTrade(ctx context.Context, id string) (execmodel.Trade, error) {
	items, err := s.ListTrades(ctx, 1000)
	if err != nil {
		return execmodel.Trade{}, err
	}
	for _, item := range items {
		if item.ID == id {
			return item, nil
		}
	}
	return execmodel.Trade{}, pgx.ErrNoRows
}

func (s *PostgresStore) ListTrades(ctx context.Context, limit int) ([]execmodel.Trade, error) {
	rows, err := s.pool.Query(ctx, `select id, mode, market_id, outcome, side, status, entry_price, exit_price, size_usd, quantity, fees_usd, slippage_usd, realized_pnl_usd, unrealized_pnl_usd, reason, opened_at, closed_at, created_at, updated_at from trades order by created_at desc limit $1`, defaultLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []execmodel.Trade{}
	for rows.Next() {
		var t execmodel.Trade
		if err := rows.Scan(&t.ID, &t.Mode, &t.MarketID, &t.Outcome, &t.Side, &t.Status, &t.EntryPrice, &t.ExitPrice, &t.SizeUSD, &t.Quantity, &t.FeesUSD, &t.SlippageUSD, &t.RealizedPnLUSD, &t.UnrealizedPnLUSD, &t.Reason, &t.OpenedAt, &t.ClosedAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *PostgresStore) SavePosition(ctx context.Context, p positionmodel.Position) error {
	_, err := s.pool.Exec(ctx, `insert into positions (id, market_id, outcome, quantity, avg_entry_price, current_price, exposure_usd, unrealized_pnl_usd, status, opened_at, updated_at)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		on conflict (id) do update set current_price=excluded.current_price, exposure_usd=excluded.exposure_usd, unrealized_pnl_usd=excluded.unrealized_pnl_usd, status=excluded.status, updated_at=excluded.updated_at`,
		p.ID, p.MarketID, p.Outcome, p.Quantity, p.AvgEntryPrice, p.CurrentPrice, p.ExposureUSD, p.UnrealizedPnLUSD, p.Status, p.OpenedAt, p.UpdatedAt)
	return err
}

func (s *PostgresStore) GetPosition(ctx context.Context, id string) (positionmodel.Position, error) {
	rows, err := s.pool.Query(ctx, `select id, market_id, outcome, quantity, avg_entry_price, current_price, exposure_usd, unrealized_pnl_usd, status, opened_at, updated_at from positions where id=$1`, id)
	if err != nil {
		return positionmodel.Position{}, err
	}
	defer rows.Close()
	items, err := scanPositions(rows)
	if err != nil {
		return positionmodel.Position{}, err
	}
	if len(items) == 0 {
		return positionmodel.Position{}, pgx.ErrNoRows
	}
	return items[0], nil
}

func (s *PostgresStore) ListPositions(ctx context.Context, status string) ([]positionmodel.Position, error) {
	rows, err := s.pool.Query(ctx, `select id, market_id, outcome, quantity, avg_entry_price, current_price, exposure_usd, unrealized_pnl_usd, status, opened_at, updated_at from positions where ($1='' or status=$1) order by opened_at desc`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanPositions(rows)
}

func (s *PostgresStore) Portfolio(ctx context.Context, startingCash float64) (riskmodel.PortfolioState, error) {
	mem := NewMemoryStore()
	trades, err := s.ListTrades(ctx, 10000)
	if err != nil {
		return riskmodel.PortfolioState{}, err
	}
	positions, err := s.ListPositions(ctx, "")
	if err != nil {
		return riskmodel.PortfolioState{}, err
	}
	for _, t := range trades {
		_ = mem.SaveTrade(ctx, t)
	}
	for _, p := range positions {
		_ = mem.SavePosition(ctx, p)
	}
	return mem.Portfolio(ctx, startingCash)
}

func (s *PostgresStore) SaveAudit(ctx context.Context, a AuditLog) error {
	_, err := s.pool.Exec(ctx, `insert into audit_logs (id, event, entity_id, payload, created_at) values ($1,$2,$3,$4,$5)`, a.ID, a.Event, a.EntityID, mustJSON(a.Payload), a.CreatedAt)
	return err
}

func (s *PostgresStore) ListAudit(ctx context.Context, limit int) ([]AuditLog, error) {
	rows, err := s.pool.Query(ctx, `select id, event, entity_id, payload, created_at from audit_logs order by created_at desc limit $1`, defaultLimit(limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []AuditLog{}
	for rows.Next() {
		var a AuditLog
		var payload []byte
		if err := rows.Scan(&a.ID, &a.Event, &a.EntityID, &payload, &a.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(payload, &a.Payload)
		out = append(out, a)
	}
	return out, rows.Err()
}

func scanMarkets(rows pgx.Rows) ([]marketmodel.Market, error) {
	out := []marketmodel.Market{}
	for rows.Next() {
		var m marketmodel.Market
		var book []byte
		if err := rows.Scan(&m.ID, &m.ConditionID, &m.Question, &m.Slug, &m.Category, &m.Active, &m.Closed, &m.EndTime, &m.Volume, &m.Liquidity, &m.YesTokenID, &m.NoTokenID, &m.YesPrice, &m.NoPrice, &m.BestBid, &m.BestAsk, &m.Spread, &book, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(book, &m.OrderBook)
		out = append(out, m)
	}
	return out, rows.Err()
}

func scanArticles(rows pgx.Rows) ([]newsmodel.NewsArticle, error) {
	out := []newsmodel.NewsArticle{}
	for rows.Next() {
		var a newsmodel.NewsArticle
		var entities, keywords []byte
		if err := rows.Scan(&a.ID, &a.Source, &a.Title, &a.URL, &a.Content, &a.Summary, &a.PublishedAt, &a.FetchedAt, &a.Hash, &entities, &keywords); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(entities, &a.Entities)
		_ = json.Unmarshal(keywords, &a.Keywords)
		out = append(out, a)
	}
	return out, rows.Err()
}

func scanProbability(rows pgx.Rows) ([]probmodel.ProbabilityDecision, error) {
	out := []probmodel.ProbabilityDecision{}
	for rows.Next() {
		var d probmodel.ProbabilityDecision
		var components []byte
		if err := rows.Scan(&d.ID, &d.MarketID, &d.NewsID, &d.MarketProbability, &d.ExecutablePrice, &d.OurProbability, &d.Edge, &d.Confidence, &components, &d.Decision, &d.Reason, &d.Side, &d.Outcome, &d.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(components, &d.Components)
		out = append(out, d)
	}
	return out, rows.Err()
}

func scanRisk(rows pgx.Rows) ([]riskmodel.RiskDecision, error) {
	out := []riskmodel.RiskDecision{}
	for rows.Next() {
		var d riskmodel.RiskDecision
		var checks []byte
		if err := rows.Scan(&d.ID, &d.MarketID, &d.ProbabilityDecisionID, &d.Approved, &d.PositionSizeUSD, &d.MaxLossUSD, &checks, &d.RejectReason, &d.Reason, &d.CreatedAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal(checks, &d.Checks)
		out = append(out, d)
	}
	return out, rows.Err()
}

func scanPositions(rows pgx.Rows) ([]positionmodel.Position, error) {
	out := []positionmodel.Position{}
	for rows.Next() {
		var p positionmodel.Position
		if err := rows.Scan(&p.ID, &p.MarketID, &p.Outcome, &p.Quantity, &p.AvgEntryPrice, &p.CurrentPrice, &p.ExposureUSD, &p.UnrealizedPnLUSD, &p.Status, &p.OpenedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func mustJSON(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal json: %v", err))
	}
	return data
}

func defaultLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	return limit
}

func capLimit[T any](items []T, limit int) []T {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}
