package emul

import (
	"errors"
	"fmt"
	"math"
)

type OrderSide string

const (
	SideBuy  OrderSide = "buy"
	SideSell OrderSide = "sell"
)

const (
	ReasonEntryLong  = "entry-long"
	ReasonEntryShort = "entry-short"
	ReasonExit       = "exit"
	ReasonStopLoss   = "stop-loss"
	ReasonLiquidate  = "liquidation"
)

type Order struct {
	ID            int64
	Symbol        string
	Side          OrderSide
	Qty           float64
	MidPrice      float64
	Price         float64
	Fee           float64
	ExecPnL       float64
	EquityBefore  float64
	Reason        string
	StopKind      string
	PositionAfter float64
	USD           float64
	ShortCash     float64
	ShortMargin   float64
	Equity        float64
	EntryPrice    float64
	Tick          int64
	PlacedTick    int64
	SpreadPct     float64
	SlippagePct   float64
}

type Balance struct {
	USD         float64
	Position    float64
	ShortCash   float64
	ShortMargin float64
	Equity      float64
	EntryPrice  float64
	LastPrice   float64
}

type Exchange struct {
	symbol       string
	fee          float64
	slippagePct  float64
	spreadPct    float64
	spreadManual bool
	prevPrice    float64
	usd          float64
	position     float64
	entryPrice   float64
	shortCash    float64
	shortMargin  float64
	lastPrice    float64
	tick         int64
	orders       []Order
	nextID       int64
	nextLimitID  int64
	pending      []pendingOrder
	executedByID map[int64]Order
	limitFailed  map[string]int
	misses       []LimitMiss
	lastBar      OHLCBar
	hasLastBar   bool
}

type pendingKind uint8

const (
	pendingOpenLong pendingKind = iota + 1
	pendingOpenShort
	pendingClose
)

type pendingOrder struct {
	id           int64
	kind         pendingKind
	price        float64
	fraction     float64
	reason       string
	stopKind     string
	placedAtTick int64
	lastReason   string
	placedBar    OHLCBar
}

type LimitMiss struct {
	Reason     string
	Kind       string
	LimitPrice float64
	PlacedTick int64
	CheckTick  int64
	PrevBar    OHLCBar
	CurrBar    OHLCBar
}

type LimitDiagnostics struct {
	PendingTotal int
	Reasons      map[string]int
	Misses       []LimitMiss
}

var (
	ErrSymbolMismatch  = errors.New("symbol mismatch")
	ErrPriceNotSet     = errors.New("price not set")
	ErrPositionOpen    = errors.New("position already open")
	ErrNoPosition      = errors.New("no open position")
	ErrInvalidFraction = errors.New("fraction must be in (0, 1]")
)

func NewExchange(symbol string, startUSD float64, fee float64, slippagePct float64, spreadPct float64) *Exchange {
	if startUSD < 0 {
		startUSD = 0
	}
	if fee < 0 {
		fee = 0
	}
	if slippagePct < 0 || slippagePct >= 1 {
		slippagePct = 0
	}
	spreadManual := false
	if spreadPct < 0 || spreadPct >= 1 {
		spreadPct = 0
	} else {
		spreadManual = true
	}
	return &Exchange{
		symbol:       symbol,
		fee:          fee,
		usd:          startUSD,
		slippagePct:  slippagePct,
		spreadPct:    spreadPct,
		spreadManual: spreadManual,
		executedByID: make(map[int64]Order),
		limitFailed:  make(map[string]int),
	}
}

func (e *Exchange) Balance() Balance {
	price := e.lastPrice
	if price <= 0 {
		price = e.entryPrice
	}
	equity := e.usd + e.shortCash + e.shortMargin
	if price > 0 {
		equity += e.position * price
	}
	return Balance{
		USD:         e.usd,
		Position:    e.position,
		ShortCash:   e.shortCash,
		ShortMargin: e.shortMargin,
		Equity:      equity,
		EntryPrice:  e.entryPrice,
		LastPrice:   e.lastPrice,
	}
}

func (e *Exchange) Orders() []Order {
	out := make([]Order, len(e.orders))
	copy(out, e.orders)
	return out
}

// tick is internal; external callers advance bars via Emulator.Next().
func (e *Exchange) tickBarAt(symbol string, tick int64, bar OHLCBar) (*Order, error) {
	if symbol != e.symbol {
		return nil, ErrSymbolMismatch
	}
	price := bar.Close
	if price <= 0 {
		return nil, fmt.Errorf("price must be positive")
	}
	if tick < 0 {
		tick = 0
	}
	e.tick = tick
	e.updateSpread(price)
	e.lastPrice = price
	executed := e.processPending(bar)
	e.lastBar = bar
	e.hasLastBar = true
	if executed != nil {
		return executed, nil
	}

	return nil, nil
}

func (e *Exchange) OpenLong(fraction float64) (*Order, error) {
	return e.openLongAtPrice(e.lastPrice, fraction, e.tick)
}

func (e *Exchange) OpenLongLimit(price float64, fraction float64) (*Order, error) {
	_, err := e.LongLimit(price, fraction)
	return nil, err
}

// LongLimit places a limit order and returns its limit-order ID.
func (e *Exchange) LongLimit(price float64, fraction float64) (int64, error) {
	if price <= 0 {
		price = e.lastPrice
	}
	if price <= 0 {
		return 0, ErrPriceNotSet
	}
	if fraction <= 0 || fraction > 1 {
		return 0, ErrInvalidFraction
	}
	e.nextLimitID++
	id := e.nextLimitID
	e.pending = append(e.pending, pendingOrder{
		id:           id,
		kind:         pendingOpenLong,
		price:        price,
		fraction:     fraction,
		placedAtTick: e.tick,
		lastReason:   "await_next_candle",
		placedBar:    e.lastBar,
	})
	return id, nil
}

func (e *Exchange) OpenShort(fraction float64) (*Order, error) {
	return e.openShortAtPrice(e.lastPrice, fraction, e.tick)
}

func (e *Exchange) OpenShortLimit(price float64, fraction float64) (*Order, error) {
	_, err := e.ShortLimit(price, fraction)
	return nil, err
}

func (e *Exchange) ShortLimit(price float64, fraction float64) (int64, error) {
	if price <= 0 {
		price = e.lastPrice
	}
	if price <= 0 {
		return 0, ErrPriceNotSet
	}
	if fraction <= 0 || fraction > 1 {
		return 0, ErrInvalidFraction
	}
	e.nextLimitID++
	id := e.nextLimitID
	e.pending = append(e.pending, pendingOrder{
		id:           id,
		kind:         pendingOpenShort,
		price:        price,
		fraction:     fraction,
		placedAtTick: e.tick,
		lastReason:   "await_next_candle",
		placedBar:    e.lastBar,
	})
	return id, nil
}

func (e *Exchange) CloseDeal(reason string) (*Order, error) {
	if e.position == 0 {
		return nil, ErrNoPosition
	}
	if e.lastPrice <= 0 {
		return nil, ErrPriceNotSet
	}
	if reason == "" {
		reason = ReasonExit
	}
	order := e.closeAtPrice(e.lastPrice, reason, "")
	order.PlacedTick = e.tick
	return &order, nil
}

// CloseDealLimit closes the current position using a caller-specified execution price (e.g. stop/limit level).
// This does not change the exchange's lastPrice for subsequent entries (it is treated like a synthetic execution level).
func (e *Exchange) CloseDealLimit(price float64, reason string, stopKind string) (*Order, error) {
	_, err := e.CloseLimit(price, reason, stopKind)
	return nil, err
}

func (e *Exchange) CloseLimit(price float64, reason string, stopKind string) (int64, error) {
	if price <= 0 {
		price = e.lastPrice
	}
	if price <= 0 {
		return 0, ErrPriceNotSet
	}
	if reason == "" {
		reason = ReasonExit
	}
	e.nextLimitID++
	id := e.nextLimitID
	e.pending = append(e.pending, pendingOrder{
		id:           id,
		kind:         pendingClose,
		price:        price,
		reason:       reason,
		stopKind:     stopKind,
		placedAtTick: e.tick,
		lastReason:   "await_next_candle",
		placedBar:    e.lastBar,
	})
	return id, nil
}

func (e *Exchange) LimitDiagnostics() LimitDiagnostics {
	out := LimitDiagnostics{
		PendingTotal: 0,
		Reasons:      make(map[string]int),
	}
	for k, v := range e.limitFailed {
		out.Reasons[k] += v
		out.PendingTotal += v
	}
	for _, p := range e.pending {
		reason := p.lastReason
		if reason == "" {
			reason = "unknown"
		}
		out.Reasons[reason]++
		out.PendingTotal++
	}
	out.Misses = append(out.Misses, e.misses...)
	return out
}

func (e *Exchange) openLongAtPrice(price float64, fraction float64, placedTick int64) (*Order, error) {
	if e.position != 0 {
		return nil, ErrPositionOpen
	}
	if e.lastPrice <= 0 {
		return nil, ErrPriceNotSet
	}
	if price <= 0 {
		price = e.lastPrice
	}
	if fraction <= 0 || fraction > 1 {
		return nil, ErrInvalidFraction
	}
	equityBefore := e.Balance().Equity
	mid := price
	notional := e.usd * fraction
	if notional <= 0 {
		return nil, ErrInvalidFraction
	}
	feeUSD := notional * e.fee
	net := notional - feeUSD
	if net <= 0 {
		return nil, ErrInvalidFraction
	}
	execPrice := e.execPrice(SideBuy, price)
	qty := net / execPrice
	execPnL := qty * (mid - execPrice)
	e.usd -= notional
	e.position = qty
	e.entryPrice = execPrice
	order := e.recordOrder(SideBuy, qty, mid, execPrice, feeUSD, execPnL, equityBefore, ReasonEntryLong, "", placedTick)
	return &order, nil
}

func (e *Exchange) openShortAtPrice(price float64, fraction float64, placedTick int64) (*Order, error) {
	if e.position != 0 {
		return nil, ErrPositionOpen
	}
	if e.lastPrice <= 0 {
		return nil, ErrPriceNotSet
	}
	if price <= 0 {
		price = e.lastPrice
	}
	if fraction <= 0 || fraction > 1 {
		return nil, ErrInvalidFraction
	}
	equityBefore := e.Balance().Equity
	mid := price
	notional := e.usd * fraction
	if notional <= 0 {
		return nil, ErrInvalidFraction
	}
	feeUSD := notional * e.fee
	net := notional - feeUSD
	if net <= 0 {
		return nil, ErrInvalidFraction
	}
	execPrice := e.execPrice(SideSell, price)
	qty := notional / execPrice
	execPnL := qty * (execPrice - mid)
	e.usd -= notional
	e.shortMargin += notional
	e.shortCash += net
	e.position = -qty
	e.entryPrice = execPrice
	order := e.recordOrder(SideSell, qty, mid, execPrice, feeUSD, execPnL, equityBefore, ReasonEntryShort, "", placedTick)
	return &order, nil
}

func (e *Exchange) processPending(bar OHLCBar) *Order {
	if len(e.pending) == 0 {
		return nil
	}
	var firstExecuted *Order
	for len(e.pending) > 0 {
		p := e.pending[0]
		if e.tick <= p.placedAtTick {
			break
		}
		if !priceInRange(p.price, bar.Low, bar.High) {
			e.pending[0].lastReason = "price_not_in_hl"
			e.limitFailed["price_not_in_hl"]++
			e.misses = append(e.misses, LimitMiss{
				Reason:     "price_not_in_hl",
				Kind:       pendingKindName(p.kind),
				LimitPrice: p.price,
				PlacedTick: p.placedAtTick,
				CheckTick:  e.tick,
				PrevBar:    p.placedBar,
				CurrBar:    bar,
			})
			break
		}
		var executed *Order
		switch p.kind {
		case pendingOpenLong:
			if e.position != 0 {
				e.limitFailed["position_state_mismatch"]++
				e.pending = e.pending[1:]
				continue
			}
			executed, _ = e.openLongAtPrice(p.price, p.fraction, p.placedAtTick)
		case pendingOpenShort:
			if e.position != 0 {
				e.limitFailed["position_state_mismatch"]++
				e.pending = e.pending[1:]
				continue
			}
			executed, _ = e.openShortAtPrice(p.price, p.fraction, p.placedAtTick)
		case pendingClose:
			if e.position == 0 {
				e.limitFailed["position_state_mismatch"]++
				e.pending = e.pending[1:]
				continue
			}
			order := e.closeAtPrice(p.price, p.reason, p.stopKind)
			order.PlacedTick = p.placedAtTick
			executed = &order
		}
		e.pending = e.pending[1:]
		if executed != nil {
			e.executedByID[p.id] = *executed
		}
		if firstExecuted == nil && executed != nil {
			firstExecuted = executed
		}
	}
	for i := 1; i < len(e.pending); i++ {
		if e.tick > e.pending[i].placedAtTick {
			e.pending[i].lastReason = "blocked_by_fifo_head"
			e.misses = append(e.misses, LimitMiss{
				Reason:     "blocked_by_fifo_head",
				Kind:       pendingKindName(e.pending[i].kind),
				LimitPrice: e.pending[i].price,
				PlacedTick: e.pending[i].placedAtTick,
				CheckTick:  e.tick,
				PrevBar:    e.pending[i].placedBar,
				CurrBar:    bar,
			})
		}
	}
	return firstExecuted
}

func pendingKindName(kind pendingKind) string {
	switch kind {
	case pendingOpenLong:
		return "open_long"
	case pendingOpenShort:
		return "open_short"
	case pendingClose:
		return "close"
	default:
		return "unknown"
	}
}

func priceInRange(price float64, low float64, high float64) bool {
	if price <= 0 || low <= 0 || high <= 0 {
		return false
	}
	if low > high {
		low, high = high, low
	}
	return price >= low && price <= high
}

func (e *Exchange) closeAtPrice(price float64, reason string, stopKind string) Order {
	// For stop closes we may execute at a synthetic "mid" (e.g., stop price) while lastPrice
	// still points to the bar's close; value equityBefore at the provided mid for consistency.
	savedLast := e.lastPrice
	e.lastPrice = price
	equityBefore := e.Balance().Equity
	mid := price
	if e.position > 0 {
		execPrice := e.execPrice(SideSell, price)
		qty := e.position
		revenue := qty * execPrice
		feeUSD := revenue * e.fee
		execPnL := qty * (execPrice - mid)
		e.usd += revenue - feeUSD
		e.position = 0
		e.entryPrice = 0
		order := e.recordOrder(SideSell, qty, mid, execPrice, feeUSD, execPnL, equityBefore, reason, stopKind, e.tick)
		e.lastPrice = savedLast
		return order
	}
	if e.position < 0 {
		execPrice := e.execPrice(SideBuy, price)
		qty := -e.position
		cost := qty * execPrice
		feeUSD := cost * e.fee
		execPnL := qty * (mid - execPrice)
		total := cost + feeUSD
		available := e.shortCash + e.shortMargin
		if available < total {
			// liquidation wipes equity
			equityBefore = e.Balance().Equity
			e.usd = 0
			e.shortCash = 0
			e.shortMargin = 0
			e.position = 0
			e.entryPrice = 0
			// Полное обнуление: PnL равен утраченной equity (без попытки компенсировать комиссию)
			execPnL = -equityBefore
			order := e.recordOrder(SideBuy, qty, mid, execPrice, feeUSD, execPnL, equityBefore, ReasonLiquidate, "", e.tick)
			e.lastPrice = savedLast
			return order
		}
		if total <= e.shortCash {
			e.shortCash -= total
		} else {
			total -= e.shortCash
			e.shortCash = 0
			e.shortMargin -= total
			if e.shortMargin < 0 {
				e.shortMargin = 0
			}
		}
		e.position = 0
		e.entryPrice = 0
		e.usd += e.shortCash + e.shortMargin
		e.shortCash = 0
		e.shortMargin = 0
		order := e.recordOrder(SideBuy, qty, mid, execPrice, feeUSD, execPnL, equityBefore, reason, stopKind, e.tick)
		e.lastPrice = savedLast
		return order
	}
	order := e.recordOrder(SideBuy, 0, mid, price, 0, 0, equityBefore, reason, stopKind, e.tick)
	e.lastPrice = savedLast
	return order
}

func (e *Exchange) applySpread(side OrderSide, price float64) float64 {
	if price <= 0 {
		return price
	}
	if e.spreadPct <= 0 {
		return price
	}
	half := e.spreadPct / 2
	switch side {
	case SideBuy:
		return price * (1 + half)
	case SideSell:
		return price * (1 - half)
	default:
		return price
	}
}

func (e *Exchange) applySlippage(side OrderSide, price float64) float64 {
	if price <= 0 {
		return price
	}
	if e.slippagePct <= 0 {
		return price
	}
	switch side {
	case SideBuy:
		return price * (1 + e.slippagePct)
	case SideSell:
		return price * (1 - e.slippagePct)
	default:
		return price
	}
}

func (e *Exchange) execPrice(side OrderSide, mid float64) float64 {
	withSpread := e.applySpread(side, mid)
	return e.applySlippage(side, withSpread)
}

func (e *Exchange) updateSpread(price float64) {
	if e.spreadManual {
		e.prevPrice = price
		return
	}
	if price <= 0 {
		return
	}
	// Simple dynamic spread model:
	// base 1bp, plus 1% of absolute return (in pct terms), clamped.
	// On daily bars this gives small widening on volatile days without exploding.
	base := 0.0001  // 1bp
	minS := 0.00005 // 0.5bp
	maxS := 0.0020  // 20bp
	extra := 0.0
	if e.prevPrice > 0 {
		ret := math.Abs(price-e.prevPrice) / e.prevPrice
		extra = ret * 0.01
	}
	s := base + extra
	if s < minS {
		s = minS
	} else if s > maxS {
		s = maxS
	}
	e.spreadPct = s
	e.prevPrice = price
}

func (e *Exchange) recordOrder(side OrderSide, qty float64, mid float64, exec float64, feeUSD float64, execPnL float64, equityBefore float64, reason string, stopKind string, placedTick int64) Order {
	e.nextID++
	bal := e.Balance()
	order := Order{
		ID:            e.nextID,
		Symbol:        e.symbol,
		Side:          side,
		Qty:           qty,
		MidPrice:      mid,
		Price:         exec,
		Fee:           feeUSD,
		ExecPnL:       execPnL,
		EquityBefore:  equityBefore,
		Reason:        reason,
		StopKind:      stopKind,
		PositionAfter: e.position,
		USD:           e.usd,
		ShortCash:     bal.ShortCash,
		ShortMargin:   bal.ShortMargin,
		Equity:        bal.Equity,
		EntryPrice:    bal.EntryPrice,
		Tick:          e.tick,
		PlacedTick:    placedTick,
		SpreadPct:     e.spreadPct,
		SlippagePct:   e.slippagePct,
	}
	e.orders = append(e.orders, order)
	return order
}
