package executionengine

import (
	"context"

	execmodel "github.com/local/polymarket-process-service/pkg/model/execution"
	positionmodel "github.com/local/polymarket-process-service/pkg/model/position"
)

type ExecutionProvider interface {
	PlaceOrder(context.Context, execmodel.OrderRequest) (execmodel.OrderResult, error)
	ClosePosition(context.Context, positionmodel.Position, string) (execmodel.OrderResult, error)
}
