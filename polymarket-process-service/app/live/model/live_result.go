package model

type LiveResult struct {
	MarketsLoaded         int              `json:"markets_loaded"`
	ArticlesLoaded        int              `json:"articles_loaded"`
	MatchesCreated        int              `json:"matches_created"`
	AICalls               int              `json:"ai_calls"`
	ProbabilityCandidates int              `json:"probability_candidates"`
	RiskApproved          int              `json:"risk_approved"`
	RiskRejected          int              `json:"risk_rejected"`
	PaperTradesOpened     int              `json:"paper_trades_opened"`
	PositionsClosed       int              `json:"positions_closed"`
	Events                []LiveAuditEvent `json:"events"`
}

type LiveAuditEvent struct {
	Type    string         `json:"type"`
	Message string         `json:"message,omitempty"`
	Payload map[string]any `json:"payload,omitempty"`
}
