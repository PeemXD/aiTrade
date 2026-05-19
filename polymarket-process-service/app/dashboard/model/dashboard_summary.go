package model

type Summary struct {
	BotStatus     string           `json:"bot_status"`
	ExecutionMode string           `json:"execution_mode"`
	Kafka         KafkaSummary     `json:"kafka"`
	Portfolio     PortfolioSummary `json:"portfolio"`
	Markets       MarketSummary    `json:"markets"`
	Signals       SignalSummary    `json:"signals"`
	Risk          RiskSummary      `json:"risk"`
	Trades        TradeSummary     `json:"trades"`
}

type KafkaSummary struct {
	Enabled bool     `json:"enabled"`
	Brokers []string `json:"brokers"`
}

type PortfolioSummary struct {
	CashUSD          float64 `json:"cash_usd"`
	EquityUSD        float64 `json:"equity_usd"`
	RealizedPnLUSD   float64 `json:"realized_pnl_usd"`
	UnrealizedPnLUSD float64 `json:"unrealized_pnl_usd"`
	OpenPositions    int     `json:"open_positions"`
}

type MarketSummary struct {
	Tracked    int `json:"tracked"`
	LiveStates int `json:"live_states"`
}

type SignalSummary struct {
	Today      int `json:"today"`
	Candidates int `json:"candidates"`
}

type RiskSummary struct {
	ApprovedToday int `json:"approved_today"`
	RejectedToday int `json:"rejected_today"`
}

type TradeSummary struct {
	OpenedToday int `json:"opened_today"`
	ClosedToday int `json:"closed_today"`
}
