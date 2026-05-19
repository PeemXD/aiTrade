package probabilityengine

import (
	aimodel "github.com/local/polymarket-process-service/pkg/model/ai"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
)

type Decision = probmodel.Decision
type CalculateRequest = probmodel.CalculateRequest
type ProbabilityDecision = probmodel.ProbabilityDecision
type AISignal = aimodel.AISignal
type Market = marketmodel.Market

const (
	DecisionNoTrade   = probmodel.DecisionNoTrade
	DecisionCandidate = probmodel.DecisionCandidate
)
