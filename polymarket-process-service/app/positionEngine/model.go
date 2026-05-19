package positionengine

import (
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
	probmodel "github.com/local/polymarket-process-service/pkg/model/probability"
)

type Position = positionmodel.Position
type ExitDecision = positionmodel.ExitDecision
type Market = marketmodel.Market
type ProbabilityDecision = probmodel.ProbabilityDecision
