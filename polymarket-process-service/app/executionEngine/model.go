package executionengine

import (
	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	marketmodel "github.com/local/polymarket-process-service/pkg/model/market"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
)

type OrderRequest = execmodel.OrderRequest
type OrderResult = execmodel.OrderResult
type Trade = execmodel.Trade
type Position = positionmodel.Position
type Market = marketmodel.Market
