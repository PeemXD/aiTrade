package model

import "time"

const (
	EventConnected                  = "connected"
	EventHeartbeat                  = "heartbeat"
	EventMarketStateUpdated         = "market_state_updated"
	EventNewsArrived                = "news_arrived"
	EventNewsMatched                = "news_matched"
	EventAISignalGenerated          = "ai_signal_generated"
	EventProbabilityDecisionCreated = "probability_decision_created"
	EventRiskApproved               = "risk_approved"
	EventRiskRejected               = "risk_rejected"
	EventPaperTradeOpened           = "paper_trade_opened"
	EventPaperTradeClosed           = "paper_trade_closed"
	EventPositionUpdated            = "position_updated"
	EventPortfolioUpdated           = "portfolio_updated"
	EventAuditCreated               = "audit_created"
	EventError                      = "error"
)

type DashboardEvent struct {
	Type      string         `json:"type"`
	EntityID  string         `json:"entity_id,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}
