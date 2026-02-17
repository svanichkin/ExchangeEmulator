package emul

import (
	"bufio"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	intervalDaily  = "d"
	intervalHourly = "h"
	intervalMinute = "m"
	minutesPerDay  = 24 * 60
)

var errNoDataRows = errors.New("no data rows parsed")

type OHLCSeries struct {
	Open  []float64
	High  []float64
	Low   []float64
	Close []float64
}

type OHLCBar struct {
	Open    float64
	High    float64
	Low     float64
	Close   float64
	Average float64
}

func BarsFromSeries(values []float64, ohlc OHLCSeries) ([]OHLCBar, error) {
	n := len(values)
	if n == 0 {
		return nil, nil
	}
	if len(ohlc.Open) != n || len(ohlc.High) != n || len(ohlc.Low) != n || len(ohlc.Close) != n {
		return nil, fmt.Errorf("ohlc length mismatch")
	}
	bars := make([]OHLCBar, n)
	for i := 0; i < n; i++ {
		bars[i] = OHLCBar{
			Open:    ohlc.Open[i],
			High:    ohlc.High[i],
			Low:     ohlc.Low[i],
			Close:   ohlc.Close[i],
			Average: values[i],
		}
	}
	return bars, nil
}

func IntervalFromFlags(useDaily bool, useHourly bool, useMinute bool) (string, error) {
	count := 0
	interval := ""
	if useDaily {
		count++
		interval = intervalDaily
	}
	if useHourly {
		count++
		interval = intervalHourly
	}
	if useMinute {
		count++
		interval = intervalMinute
	}
	if count == 0 {
		return "", fmt.Errorf("select interval with -d, -h, or -m")
	}
	if count > 1 {
		return "", fmt.Errorf("only one interval flag is allowed: -d, -h, or -m")
	}
	return interval, nil
}

func PointsPerDayForInterval(interval string) int {
	switch interval {
	case intervalDaily:
		return 1
	case intervalHourly:
		return 24
	case intervalMinute:
		return minutesPerDay
	default:
		return 0
	}
}

func LoadSeriesFromDataRoot(dataRoot string, coin string, interval string) ([]float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, 0, err
	}
	if !info.IsDir() {
		return nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFiles(dir)
	if err != nil {
		return nil, 0, err
	}
	return loadSeriesFromFiles(dir, files, nil)
}

func LoadSeriesFromDataRootMonths(dataRoot string, coin string, interval string, months []int) ([]float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, 0, err
	}
	if !info.IsDir() {
		return nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFiles(dir)
	if err != nil {
		return nil, 0, err
	}
	return loadSeriesFromFiles(dir, files, buildMonthFilter(months))
}

func LoadSeriesFromDataRootYears(dataRoot string, coin string, interval string, years []int) ([]float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, 0, err
	}
	if !info.IsDir() {
		return nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFilesForYears(dir, coin, years)
	if err != nil {
		return nil, 0, err
	}
	return loadSeriesFromFiles(dir, files, nil)
}

func LoadSeriesFromDataRootYearsMonths(dataRoot string, coin string, interval string, years []int, months []int) ([]float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, 0, err
	}
	if !info.IsDir() {
		return nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFilesForYears(dir, coin, years)
	if err != nil {
		return nil, 0, err
	}
	return loadSeriesFromFiles(dir, files, buildMonthFilter(months))
}

func LoadSeriesWithCloseFromDataRoot(dataRoot string, coin string, interval string) ([]float64, []float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, nil, 0, err
	}
	if !info.IsDir() {
		return nil, nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFiles(dir)
	if err != nil {
		return nil, nil, 0, err
	}
	return loadSeriesFromFilesWithClose(dir, files, nil)
}

func LoadSeriesWithCloseFromDataRootMonths(dataRoot string, coin string, interval string, months []int) ([]float64, []float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, nil, 0, err
	}
	if !info.IsDir() {
		return nil, nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFiles(dir)
	if err != nil {
		return nil, nil, 0, err
	}
	return loadSeriesFromFilesWithClose(dir, files, buildMonthFilter(months))
}

func LoadSeriesWithCloseFromDataRootYears(dataRoot string, coin string, interval string, years []int) ([]float64, []float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, nil, 0, err
	}
	if !info.IsDir() {
		return nil, nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFilesForYears(dir, coin, years)
	if err != nil {
		return nil, nil, 0, err
	}
	return loadSeriesFromFilesWithClose(dir, files, nil)
}

func LoadSeriesWithCloseFromDataRootYearsMonths(dataRoot string, coin string, interval string, years []int, months []int) ([]float64, []float64, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, nil, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, nil, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, nil, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, nil, 0, err
	}
	if !info.IsDir() {
		return nil, nil, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFilesForYears(dir, coin, years)
	if err != nil {
		return nil, nil, 0, err
	}
	return loadSeriesFromFilesWithClose(dir, files, buildMonthFilter(months))
}

func LoadSeriesWithOHLCFromDataRoot(dataRoot string, coin string, interval string) ([]float64, OHLCSeries, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, OHLCSeries{}, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	if !info.IsDir() {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFiles(dir)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	return loadSeriesFromFilesWithOHLC(dir, files, nil)
}

func LoadSeriesWithOHLCFromDataRootMonths(dataRoot string, coin string, interval string, months []int) ([]float64, OHLCSeries, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, OHLCSeries{}, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	if !info.IsDir() {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFiles(dir)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	return loadSeriesFromFilesWithOHLC(dir, files, buildMonthFilter(months))
}

func LoadSeriesWithOHLCFromDataRootYears(dataRoot string, coin string, interval string, years []int) ([]float64, OHLCSeries, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, OHLCSeries{}, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	if !info.IsDir() {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFilesForYears(dir, coin, years)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	return loadSeriesFromFilesWithOHLC(dir, files, nil)
}

func LoadSeriesWithOHLCFromDataRootYearsMonths(dataRoot string, coin string, interval string, years []int, months []int) ([]float64, OHLCSeries, float64, error) {
	root := strings.TrimSpace(dataRoot)
	if root == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data root is empty")
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	if coin == "" {
		return nil, OHLCSeries{}, 0, fmt.Errorf("coin is empty")
	}
	interval = strings.ToLower(strings.TrimSpace(interval))
	switch interval {
	case intervalDaily, intervalHourly, intervalMinute:
	default:
		return nil, OHLCSeries{}, 0, fmt.Errorf("invalid interval %q", interval)
	}

	dir := filepath.Join(root, coin, interval)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	if !info.IsDir() {
		return nil, OHLCSeries{}, 0, fmt.Errorf("data path is not a directory: %s", dir)
	}

	files, err := listCSVFilesForYears(dir, coin, years)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	return loadSeriesFromFilesWithOHLC(dir, files, buildMonthFilter(months))
}

func loadSeriesFromFiles(dir string, files []string, months map[int]bool) ([]float64, float64, error) {
	if len(files) == 0 {
		return nil, 0, fmt.Errorf("no csv files found in %s", dir)
	}

	series := make([]float64, 0, 1024)
	maxValue := math.Inf(-1)
	for _, filePath := range files {
		values, maxLocal, err := loadSeriesFromCSV(filePath, months)
		if err != nil {
			if errors.Is(err, errNoDataRows) {
				continue
			}
			return nil, 0, err
		}
		series = append(series, values...)
		if maxLocal > maxValue {
			maxValue = maxLocal
		}
	}
	if len(series) == 0 {
		return nil, 0, fmt.Errorf("no data loaded from %s", dir)
	}
	if math.IsInf(maxValue, -1) {
		maxValue = 0
	}
	return series, maxValue, nil
}

func loadSeriesFromFilesWithClose(dir string, files []string, months map[int]bool) ([]float64, []float64, float64, error) {
	if len(files) == 0 {
		return nil, nil, 0, fmt.Errorf("no csv files found in %s", dir)
	}

	series := make([]float64, 0, 1024)
	closeSeries := make([]float64, 0, 1024)
	maxValue := math.Inf(-1)
	for _, filePath := range files {
		values, closes, maxLocal, err := loadSeriesFromCSVWithClose(filePath, months)
		if err != nil {
			if errors.Is(err, errNoDataRows) {
				continue
			}
			return nil, nil, 0, err
		}
		series = append(series, values...)
		closeSeries = append(closeSeries, closes...)
		if maxLocal > maxValue {
			maxValue = maxLocal
		}
	}
	if len(series) == 0 {
		return nil, nil, 0, fmt.Errorf("no data loaded from %s", dir)
	}
	if len(series) != len(closeSeries) {
		return nil, nil, 0, fmt.Errorf("series length mismatch for %s", dir)
	}
	if math.IsInf(maxValue, -1) {
		maxValue = 0
	}
	return series, closeSeries, maxValue, nil
}

func loadSeriesFromFilesWithOHLC(dir string, files []string, months map[int]bool) ([]float64, OHLCSeries, float64, error) {
	if len(files) == 0 {
		return nil, OHLCSeries{}, 0, fmt.Errorf("no csv files found in %s", dir)
	}

	series := make([]float64, 0, 1024)
	ohlc := OHLCSeries{
		Open:  make([]float64, 0, 1024),
		High:  make([]float64, 0, 1024),
		Low:   make([]float64, 0, 1024),
		Close: make([]float64, 0, 1024),
	}
	maxValue := math.Inf(-1)
	for _, filePath := range files {
		values, fileOHLC, maxLocal, err := loadSeriesFromCSVWithOHLC(filePath, months)
		if err != nil {
			if errors.Is(err, errNoDataRows) {
				continue
			}
			return nil, OHLCSeries{}, 0, err
		}
		series = append(series, values...)
		ohlc.Open = append(ohlc.Open, fileOHLC.Open...)
		ohlc.High = append(ohlc.High, fileOHLC.High...)
		ohlc.Low = append(ohlc.Low, fileOHLC.Low...)
		ohlc.Close = append(ohlc.Close, fileOHLC.Close...)
		if maxLocal > maxValue {
			maxValue = maxLocal
		}
	}
	if len(series) == 0 {
		return nil, OHLCSeries{}, 0, fmt.Errorf("no data loaded from %s", dir)
	}
	if len(series) != len(ohlc.Close) {
		return nil, OHLCSeries{}, 0, fmt.Errorf("series length mismatch for %s", dir)
	}
	if math.IsInf(maxValue, -1) {
		maxValue = 0
	}
	return series, ohlc, maxValue, nil
}

func listCSVFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.ToLower(filepath.Ext(name)) != ".csv" {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}
	sort.Strings(files)
	return files, nil
}

func listCSVFilesForYears(dir string, coin string, years []int) ([]string, error) {
	if len(years) == 0 {
		return listCSVFiles(dir)
	}
	coin = strings.ToLower(strings.TrimSpace(coin))
	files := make([]string, 0, len(years))
	for _, year := range years {
		if year <= 0 {
			continue
		}
		yearOnly := filepath.Join(dir, fmt.Sprintf("%d.csv", year))
		coinYear := ""
		if coin != "" {
			coinYear = filepath.Join(dir, fmt.Sprintf("%s%d.csv", coin, year))
		}
		if path, ok, err := resolveYearFile(yearOnly, coinYear); err != nil {
			return nil, err
		} else if ok {
			files = append(files, path)
			continue
		}
		return nil, fmt.Errorf("missing year file %d (expected %s or %s)", year, filepath.Base(yearOnly), filepath.Base(coinYear))
	}
	sort.Strings(files)
	return files, nil
}

func resolveYearFile(yearOnly string, coinYear string) (string, bool, error) {
	if coinYear != "" {
		if info, err := os.Stat(coinYear); err == nil {
			if info.IsDir() {
				return "", false, fmt.Errorf("data path is a directory: %s", coinYear)
			}
			return coinYear, true, nil
		} else if !os.IsNotExist(err) {
			return "", false, err
		}
	}
	if info, err := os.Stat(yearOnly); err == nil {
		if info.IsDir() {
			return "", false, fmt.Errorf("data path is a directory: %s", yearOnly)
		}
		return yearOnly, true, nil
	} else if !os.IsNotExist(err) {
		return "", false, err
	}
	return "", false, nil
}

func loadSeriesFromCSV(path string, months map[int]bool) ([]float64, float64, error) {
	values, _, maxValue, err := loadSeriesFromCSVWithClose(path, months)
	return values, maxValue, err
}

func loadSeriesFromCSVWithClose(path string, months map[int]bool) ([]float64, []float64, float64, error) {
	values, ohlc, maxValue, err := loadSeriesFromCSVWithOHLC(path, months)
	if err != nil {
		return nil, nil, 0, err
	}
	return values, ohlc.Close, maxValue, nil
}

func loadSeriesFromCSVWithOHLC(path string, months map[int]bool) ([]float64, OHLCSeries, float64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	values := make([]float64, 0, 1024)
	ohlc := OHLCSeries{
		Open:  make([]float64, 0, 1024),
		High:  make([]float64, 0, 1024),
		Low:   make([]float64, 0, 1024),
		Close: make([]float64, 0, 1024),
	}
	maxValue := math.Inf(-1)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 6 {
			continue
		}
		if months != nil {
			ts, ok := parseCSVTime(parts[0])
			if !ok {
				continue
			}
			if !months[int(ts.Month())] {
				continue
			}
		}
		openValue, ok := parseCSVFloat(parts[1])
		if !ok {
			continue
		}
		highValue, ok := parseCSVFloat(parts[2])
		if !ok {
			continue
		}
		lowValue, ok := parseCSVFloat(parts[3])
		if !ok {
			continue
		}
		closeValue, ok := parseCSVFloat(parts[4])
		if !ok {
			continue
		}
		value := (openValue + highValue + lowValue + closeValue) / 4
		values = append(values, value)
		ohlc.Open = append(ohlc.Open, openValue)
		ohlc.High = append(ohlc.High, highValue)
		ohlc.Low = append(ohlc.Low, lowValue)
		ohlc.Close = append(ohlc.Close, closeValue)
		if value > maxValue {
			maxValue = value
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, OHLCSeries{}, 0, err
	}
	if len(values) == 0 {
		return nil, OHLCSeries{}, 0, fmt.Errorf("%s: %w", path, errNoDataRows)
	}
	if math.IsInf(maxValue, -1) {
		maxValue = 0
	}
	return values, ohlc, maxValue, nil
}

func buildMonthFilter(months []int) map[int]bool {
	if len(months) == 0 {
		return nil
	}
	filter := make(map[int]bool, len(months))
	for _, month := range months {
		if month >= 1 && month <= 12 {
			filter[month] = true
		}
	}
	if len(filter) == 0 {
		return nil
	}
	return filter
}

func parseCSVTime(raw string) (time.Time, bool) {
	value := strings.TrimSpace(raw)
	value = strings.Trim(value, "\"")
	if value == "" {
		return time.Time{}, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		floatVal, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return time.Time{}, false
		}
		parsed = int64(floatVal)
	}
	sec := parsed
	if parsed > 1_000_000_000_000 {
		sec = parsed / 1000
	}
	if sec <= 0 {
		return time.Time{}, false
	}
	return time.Unix(sec, 0).UTC(), true
}

func parseCSVFloat(raw string) (float64, bool) {
	value := strings.TrimSpace(raw)
	value = strings.Trim(value, "\"")
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, false
	}
	return parsed, true
}
