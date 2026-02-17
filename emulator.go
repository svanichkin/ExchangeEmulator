package emul

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

var ErrNoMoreBars = errors.New("no more bars")

// Emulator replays historical bars one-by-one and applies them to Exchange.
type Emulator struct {
	bars  []OHLCBar
	index int
	ex    *Exchange
}

type EmulatorConfig struct {
	StartUSD    float64
	Fee         float64
	SlippagePct float64
	SpreadPct   float64
	Bars        []OHLCBar
}

func NewEmulator(startUSD float64, fee float64, slippagePct float64, spreadPct float64, bars []OHLCBar) (*Emulator, error) {
	if len(bars) == 0 {
		return nil, fmt.Errorf("bars are empty")
	}
	return &Emulator{
		bars: bars,
		ex:   NewExchange(startUSD, fee, slippagePct, spreadPct),
	}, nil
}

func NewEmulatorFromCSV(startUSD float64, fee float64, slippagePct float64, spreadPct float64, csvPath string) (*Emulator, error) {
	bars, err := LoadBarsFromCSV(csvPath)
	if err != nil {
		return nil, err
	}
	return NewEmulator(startUSD, fee, slippagePct, spreadPct, bars)
}

// NewEmulatorFromConfig consumes prepared bars (no file I/O in production code paths).
func NewEmulatorFromConfig(cfg EmulatorConfig) (*Emulator, error) {
	return NewEmulator(
		cfg.StartUSD,
		cfg.Fee,
		cfg.SlippagePct,
		cfg.SpreadPct,
		cfg.Bars,
	)
}

func LoadBarsFromCSV(csvPath string) ([]OHLCBar, error) {
	path := strings.TrimSpace(csvPath)
	if path == "" {
		return nil, fmt.Errorf("csv path is empty")
	}
	if strings.ToLower(filepath.Ext(path)) != ".csv" {
		return nil, fmt.Errorf("csv path must end with .csv")
	}
	values, ohlc, _, err := loadSeriesFromCSVWithOHLC(path, nil)
	if err != nil {
		return nil, err
	}
	return BarsFromSeries(values, ohlc)
}

func (e *Emulator) Next() (OHLCBar, []Order, error) {
	if e.index >= len(e.bars) {
		return OHLCBar{}, nil, ErrNoMoreBars
	}
	bar := e.bars[e.index]
	before := e.ex.Orders()
	_, err := e.ex.tickBarAt(int64(e.index+1), bar)
	if err != nil {
		return OHLCBar{}, nil, err
	}
	after := e.ex.Orders()
	executed := make([]Order, 0)
	if len(after) > len(before) {
		executed = append(executed, after[len(before):]...)
	}
	e.index++
	return bar, executed, nil
}

func (e *Emulator) Exchange() *Exchange {
	return e.ex
}

func (e *Emulator) Bars() []OHLCBar {
	out := make([]OHLCBar, len(e.bars))
	copy(out, e.bars)
	return out
}
