# ExchangeEmulator

A lightweight Go package that emulates trade execution (long/short, limit orders, fees, slippage/spread) and loads historical price series from CSV files.

## Features

- execution against OHLC bars;
- open/close long and short positions;
- limit orders plus diagnostics for missed executions;
- balance, equity, and order history tracking;
- loading price series (`d`, `h`, `m`) from a folder structure.

## Requirements

- Go `1.25+` (set in `go.mod`)

## Installation

```bash
go get github.com/svanichkin/ExchangeEmulator
```

## Expected data layout

```
<data_root>/
  btc/
    d/
      btc-usd-max-2024.csv
      ...
    h/
      ...
    m/
      ...
```

## Initializing the data path

Loader functions accept the absolute path to your data root (`dataRoot`).
Inside it, the library expects `<dataRoot>/<coin>/<interval>/*.csv`.

## Quick check

```bash
go test ./...
```

## Notes for publishing on GitHub

- module path is `github.com/svanichkin/ExchangeEmulator`;
- dependencies are trimmed to those actually used;
- `.gitignore` includes local Go caches.
