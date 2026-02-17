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
	symbol string
	bars   []OHLCBar
	index  int
	ex     *Exchange
}

type EmulatorConfig struct {
	Symbol      string
	StartUSD    float64
	Fee         float64
	SlippagePct float64
	SpreadPct   float64
	CSVPath     string
}

func NewEmulator(symbol string, startUSD float64, fee float64, slippagePct float64, spreadPct float64, bars []OHLCBar) (*Emulator, error) {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return nil, fmt.Errorf("symbol is empty")
	}
	if len(bars) == 0 {
		return nil, fmt.Errorf("bars are empty")
	}
	return &Emulator{
		symbol: symbol,
		bars:   bars,
		ex:     NewExchange(symbol, startUSD, fee, slippagePct, spreadPct),
	}, nil
}

func NewEmulatorFromCSV(symbol string, startUSD float64, fee float64, slippagePct float64, spreadPct float64, csvPath string) (*Emulator, error) {
	bars, err := LoadBarsFromCSV(csvPath)
	if err != nil {
		return nil, err
	}
	return NewEmulator(symbol, startUSD, fee, slippagePct, spreadPct, bars)
}

// NewEmulatorFromConfig groups path and fee together to reduce call-site mistakes.
func NewEmulatorFromConfig(cfg EmulatorConfig) (*Emulator, error) {
	return NewEmulatorFromCSV(
		cfg.Symbol,
		cfg.StartUSD,
		cfg.Fee,
		cfg.SlippagePct,
		cfg.SpreadPct,
		cfg.CSVPath,
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
	_, err := e.ex.tickBarAt(e.symbol, int64(e.index+1), bar)
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
