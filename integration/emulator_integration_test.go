package emul_test

import (
	emul "github.com/svanichkin/ExchangeEmulator"
	"testing"
)

const integrationCSVPath = "/Users/alien/Vault/Projects/Self/Trading/PriceLoader/data/enj/h/enj2026.csv"

func inRange(price float64, low float64, high float64) bool {
	if low > high {
		low, high = high, low
	}
	return price >= low && price <= high
}

func TestIntegrationNextLogsTenBars(t *testing.T) {
	emu, err := emul.NewEmulatorFromConfig(emul.EmulatorConfig{
		Symbol:      "enj",
		StartUSD:    1000,
		Fee:         0.001,
		SlippagePct: 0,
		SpreadPct:   0,
		CSVPath:     integrationCSVPath,
	})
	if err != nil {
		t.Fatalf("new emulator: %v", err)
	}

	for i := 0; i < 10; i++ {
		bar, orders, err := emu.Next()
		if err != nil {
			t.Fatalf("next %d: %v", i+1, err)
		}
		t.Logf("next=%d O=%.8f H=%.8f L=%.8f C=%.8f executed=%d", i+1, bar.Open, bar.High, bar.Low, bar.Close, len(orders))
		for j, order := range orders {
			t.Logf("next=%d exec[%d]: reason=%s side=%s price=%.8f qty=%.8f tick=%d", i+1, j, order.Reason, order.Side, order.Price, order.Qty, order.Tick)
		}
	}
}

func TestIntegrationLimitAndOppositeOrder(t *testing.T) {
	emu, err := emul.NewEmulatorFromConfig(emul.EmulatorConfig{
		Symbol:      "enj",
		StartUSD:    1000,
		Fee:         0.001,
		SlippagePct: 0,
		SpreadPct:   0,
		CSVPath:     integrationCSVPath,
	})
	if err != nil {
		t.Fatalf("new emulator: %v", err)
	}

	bars := emu.Bars()
	if len(bars) < 4 {
		t.Fatalf("need at least 4 bars, got %d", len(bars))
	}

	bar1, _, err := emu.Next()
	if err != nil {
		t.Fatalf("prime first bar: %v", err)
	}
	t.Logf("step 1 | next() -> OHLC O=%.8f H=%.8f L=%.8f C=%.8f", bar1.Open, bar1.High, bar1.Low, bar1.Close)

	limitPrice := bars[1].Average
	limitID, err := emu.Exchange().LongLimit(limitPrice, 1.0)
	if err != nil {
		t.Fatalf("place long limit: %v", err)
	}
	t.Logf("step 2 | longLimit(%.8f) -> id=%d", limitPrice, limitID)

	bar2, orders1, err := emu.Next()
	if err != nil {
		t.Fatalf("next after long limit #1: %v", err)
	}
	t.Logf("step 3 | next() -> OHLC O=%.8f H=%.8f L=%.8f C=%.8f executed=%d", bar2.Open, bar2.High, bar2.Low, bar2.Close, len(orders1))
	t.Logf("step 3 | priceInRange(id=%d, p.price=%.8f, low=%.8f, high=%.8f) -> %v", limitID, limitPrice, bar2.Low, bar2.High, inRange(limitPrice, bar2.Low, bar2.High))
	// executions are implicit; priceInRange + executed count above is enough

	bar3, _, err := emu.Next()
	if err != nil {
		t.Fatalf("next after long limit #2: %v", err)
	} // no executions expected here
	if len(orders1) != 1 {
		t.Fatalf("expected exactly one long execution after two next() calls, got %d", len(orders1))
	}
	if orders1[0].Reason != emul.ReasonEntryLong {
		t.Fatalf("expected first executed order reason=%q, got %q", emul.ReasonEntryLong, orders1[0].Reason)
	}
	t.Logf("step 4 | next() -> OHLC O=%.8f H=%.8f L=%.8f C=%.8f executed=0 (no pending limits)", bar3.Open, bar3.High, bar3.Low, bar3.Close)
	oppositePrice := bars[3].Average
	closeID, err := emu.Exchange().CloseLimit(oppositePrice, emul.ReasonExit, "flip-close")
	if err != nil {
		t.Fatalf("place opposite close limit: %v", err)
	}
	shortID, err := emu.Exchange().ShortLimit(oppositePrice, 1.0)
	if err != nil {
		t.Fatalf("place opposite short limit: %v", err)
	}
	t.Logf("step 5 | closeLimit(%.8f) -> id=%d", oppositePrice, closeID)
	t.Logf("step 6 | shortLimit(%.8f) -> id=%d", oppositePrice, shortID)

	bar4, closeOrders, err := emu.Next()
	if err != nil {
		t.Fatalf("next after opposite order: %v", err)
	}
	t.Logf("step 7 | next() -> OHLC O=%.8f H=%.8f L=%.8f C=%.8f executed=%d", bar4.Open, bar4.High, bar4.Low, bar4.Close, len(closeOrders))
	t.Logf("step 7 | priceInRange(id=%d, p.price=%.8f, low=%.8f, high=%.8f) -> %v", closeID, oppositePrice, bar4.Low, bar4.High, inRange(oppositePrice, bar4.Low, bar4.High))
	t.Logf("step 7 | priceInRange(id=%d, p.price=%.8f, low=%.8f, high=%.8f) -> %v", shortID, oppositePrice, bar4.Low, bar4.High, inRange(oppositePrice, bar4.Low, bar4.High))
	if len(closeOrders) != 2 {
		t.Fatalf("expected exactly two opposite executions on next() (close then short), got %d", len(closeOrders))
	}
	if closeOrders[0].Reason != emul.ReasonExit {
		t.Fatalf("expected first order reason=%q, got %q", emul.ReasonExit, closeOrders[0].Reason)
	}
	if closeOrders[1].Reason != emul.ReasonEntryShort {
		t.Fatalf("expected second order reason=%q, got %q", emul.ReasonEntryShort, closeOrders[1].Reason)
	}

	// final status implied by executed=2 and ordering: first close, then short
	t.Logf("step 8 | final status: close id=%d executed=true; short id=%d executed=true", closeID, shortID)
}
