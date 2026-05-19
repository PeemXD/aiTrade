package errors

import "errors"

var (
	ErrNotFound       = errors.New("not found")
	ErrInvalidInput   = errors.New("invalid input")
	ErrAIUnavailable  = errors.New("ai disabled or unavailable")
	ErrRiskRejected   = errors.New("risk rejected")
	ErrRealTradingOff = errors.New("real trading disabled")
)
