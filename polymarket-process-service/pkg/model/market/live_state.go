package model

// ApplyLiveState overlays the latest websocket-derived state onto a stored
// market record. Static market metadata remains on the Market value, while
// executable prices and order book levels come from the live cache when present.
func ApplyLiveState(m Market, st LiveMarketState) Market {
	if st.MarketID == "" {
		return m
	}
	if st.BestBid > 0 {
		m.BestBid = st.BestBid
	}
	if st.BestAsk > 0 {
		m.BestAsk = st.BestAsk
	}
	if st.MidPrice > 0 {
		m.YesPrice = st.MidPrice
	} else if st.LastPrice > 0 {
		m.YesPrice = st.LastPrice
	}
	if st.Spread > 0 {
		m.Spread = st.Spread
	} else if m.BestBid > 0 && m.BestAsk > 0 {
		m.Spread = m.BestAsk - m.BestBid
	}
	if len(st.OrderBookBids) > 0 {
		m.OrderBook.Bids = st.OrderBookBids
	}
	if len(st.OrderBookAsks) > 0 {
		m.OrderBook.Asks = st.OrderBookAsks
	}
	if !st.UpdatedAt.IsZero() {
		m.UpdatedAt = st.UpdatedAt
	}
	return m
}
