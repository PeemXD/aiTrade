package kafka

import "strings"

const (
	TopicMarketSelected         = "market.selected"
	TopicMarketPriceUpdated     = "market.price.updated"
	TopicMarketOrderBookUpdated = "market.orderbook.updated"
	TopicMarketTradeExecuted    = "market.trade.executed"

	TopicNewsArrived       = "news.arrived"
	TopicNewsDeduplicated  = "news.deduplicated"
	TopicNewsMarketMatched = "news.market.matched"

	TopicAISignalGenerated = "ai.signal.generated"

	TopicProbabilityNoTrade   = "probability.no_trade"
	TopicProbabilityCandidate = "probability.candidate"

	TopicRiskApproved = "risk.approved"
	TopicRiskRejected = "risk.rejected"

	TopicTradeOpened   = "trade.opened"
	TopicTradeClosed   = "trade.closed"
	TopicTradeRejected = "trade.rejected"

	TopicPositionUpdated       = "position.updated"
	TopicPositionExitCandidate = "position.exit_candidate"

	TopicPortfolioUpdated = "portfolio.updated"
	TopicAuditCreated     = "audit.created"

	TopicDLQMarket  = "dlq.market"
	TopicDLQNews    = "dlq.news"
	TopicDLQProcess = "dlq.process"
	TopicDLQTrade   = "dlq.trade"
)

var AllBusinessTopics = []string{
	TopicMarketSelected,
	TopicMarketPriceUpdated,
	TopicMarketOrderBookUpdated,
	TopicMarketTradeExecuted,
	TopicNewsArrived,
	TopicNewsDeduplicated,
	TopicNewsMarketMatched,
	TopicAISignalGenerated,
	TopicProbabilityNoTrade,
	TopicProbabilityCandidate,
	TopicRiskApproved,
	TopicRiskRejected,
	TopicTradeOpened,
	TopicTradeClosed,
	TopicTradeRejected,
	TopicPositionUpdated,
	TopicPositionExitCandidate,
	TopicPortfolioUpdated,
	TopicAuditCreated,
}

func DLQTopic(topic string) string {
	switch {
	case strings.HasPrefix(topic, "market."):
		return TopicDLQMarket
	case strings.HasPrefix(topic, "news."):
		return TopicDLQNews
	case strings.HasPrefix(topic, "trade."), strings.HasPrefix(topic, "position."), strings.HasPrefix(topic, "portfolio."):
		return TopicDLQTrade
	default:
		return TopicDLQProcess
	}
}
