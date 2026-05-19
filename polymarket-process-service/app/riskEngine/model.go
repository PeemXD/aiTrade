package riskengine

import (
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	riskmodel "github.com/local/polymarket-process-service/pkg/model/risk"
)

type EvaluateRequest = riskmodel.EvaluateRequest
type PortfolioState = riskmodel.PortfolioState
type RiskDecision = riskmodel.RiskDecision
type Market = marketmodel.Market
