package main

import (
	"fmt"
	"github.com/graphite-ng/carbon-relay-ng/aggregator"
	carbon "github.com/marpaia/graphite-golang"
	"github.com/vimeo/graphite-go"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func main() {

	var usage = `gab <graphite> <carbon> <regex> <out> <func> <from> <to>
    graphite: http://mygraphiteserver
    carbon: yourcarbonhost:2003
    regex: regex to match incoming metrics
    out:   pattern to construct outgoing metric
    func:  function to use. avg or sumk
    from:  from unix timestamp (default: 0)
    to:    to unix timestamp (default: now)
    `
	args := os.Args[1:]
	if len(args) != 7 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(1)
	}

	graphiteUrl := args[0]
	graphiteClient, err := graphite.NewUrl(graphiteUrl)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Can't configure graphite client:", err)
		os.Exit(1)
	}

	fields := strings.Split(args[1], ":")
	host := fields[0]
	port := 2003
	if len(fields) == 2 {
		port, err = strconv.Atoi(fields[1])
		if err != nil {
			fmt.Fprintln(os.Stderr, "Invalid carbon port:", err)
			os.Exit(1)
		}

	}
	carbonClient, err := carbon.NewGraphite(host, port)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Can't connect to carbon server:", err)
		os.Exit(1)
	}

	regex, err := regexp.Compile(args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "Invalid regex. Can't compile:", err)
		os.Exit(1)
	}

	outFmt := args[3]

	fn, ok := aggregator.Funcs[args[4]]
	if !ok {
		fmt.Fprintf(os.Stderr, "no such aggregation function '%s'\n", args[4])
		os.Exit(1)
	}

	var fromUnix, toUnix int
	from := args[5]
	if from == "" {
		fromUnix = 0
	} else {
		fromUnix, err = strconv.Atoi(from)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Can't parse from date:", err)
			os.Exit(1)
		}
	}

	to := args[6]
	if to == "" {
		toUnix = int(time.Now().Unix())
	} else {
		toUnix, err = strconv.Atoi(to)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Can't parse to date:", err)
			os.Exit(1)
		}
	}

	a := NewAggregator(graphiteClient, carbonClient, fn, regex, outFmt)
	a.Process(fromUnix, toUnix)
}

type Aggregation struct {
	values []float64
}
type Aggregator struct {
	graphiteClient *graphite.Graphite
	carbonClient   *carbon.Graphite
	fn             aggregator.Func
	aggregations   map[string]*Aggregation // by outKey
	index          map[string]*Aggregation // by metric
}

func NewAggregator(graphiteClient *graphite.Graphite, carbonClient *carbon.Graphite, fn aggregator.Func, regex *regexp.Regexp, outFmt string) *Aggregator {
	fmt.Println("querying graphite for metrics..")
	metrics, err := graphiteClient.Metrics()
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not fetch list of metrics", err)
		os.Exit(1)
	}

	a := &Aggregator{
		graphiteClient,
		carbonClient,
		fn,
		make(map[string]*Aggregation),
		make(map[string]*Aggregation),
	}

	fmt.Println("figuring out which metrics match")
	for _, metric := range metrics {
		matches := regex.FindStringSubmatchIndex(metric)
		if len(matches) == 0 {
			continue
		}
		var dst []byte
		outKey := string(regex.ExpandString(dst, outFmt, metric, matches))
		if _, ok := a.aggregations[outKey]; ok {
			//agg.metrics = append(agg.metrics, outKey)
		} else {
			a.aggregations[outKey] = &Aggregation{
				//   make([]string, 0),
				make([]float64, 0),
			}
		}

		// update index
		a.index[metric] = a.aggregations[outKey]
	}

	return a
}

func (a *Aggregator) Metrics() (metrics []string) {
	for metric, _ := range a.index {
		metrics = append(metrics, metric)
	}
	return
}

func (a *Aggregator) Process(fromUnix, toUnix int) {
	start := time.Unix(int64(fromUnix), 0)
	end := time.Unix(int64(toUnix), 0)
	req := &graphite.Request{
		&start,
		&end,
		a.Metrics(),
	}
	fmt.Println("requesting", len(req.Targets), "metrics")
	resp, err := a.graphiteClient.Query(req)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not query graphite for data", err)
		os.Exit(1)
	}
	fmt.Println("response has", len(resp), "series in it")
	if len(resp) == 0 {
		fmt.Println("no series -> nothing to do")
		os.Exit(0)
	}
	// some basic sanity checks. each target must have equal amount of values, and ts of first and last should be the same
	amount := len(resp[0].Datapoints)
	if amount == 0 {
		fmt.Println("first series has no points -> nothing to do")
		os.Exit(0)
	}
	firstTs := resp[0].Datapoints[0][1]
	lastTs := resp[0].Datapoints[amount-1][1]
	for i, series := range resp {
		if len(series.Datapoints) != amount {
			fmt.Fprintf(os.Stderr, "series 1 has %d points, whereas series %d has %d datapoints\n", amount, i+1, len(series.Datapoints))
			fmt.Fprintf(os.Stderr, "request from: %d -- to: %d\n", fromUnix, toUnix)
			fmt.Fprintf(os.Stderr, "series 1: %s -- points: %v\n", resp[0].Target, resp[0].Datapoints)
			fmt.Fprintf(os.Stderr, "series %d: %s -- points: %v\n", i+1, series.Target, series.Datapoints)
			os.Exit(2)
		}
		if series.Datapoints[0][1] != firstTs {
			fmt.Fprintf(os.Stderr, "series 1 first point is @ %d, whereas series %d is at @ %d\n", firstTs, i+1, series.Datapoints[0][1])
			os.Exit(2)
		}
		if series.Datapoints[len(series.Datapoints)-1][1] != lastTs {
			fmt.Fprintf(os.Stderr, "series 1 last point is @ %d, whereas series %d is at @ %d\n", lastTs, i+1, series.Datapoints[len(series.Datapoints)-1][1])
			os.Exit(2)
		}
	}

	// now do the actual computations
	for point := 0; point < amount; point++ {
		for _, agg := range a.aggregations {
			agg.values = make([]float64, 0)
		}
		for _, series := range resp {
			// points can be {"", "int"} (null value + timestamp) or {"float", "int"}
			value, err := series.Datapoints[point][0].Float64()
			if err != nil {
				// this should be a null value, and just like graphite does, we'll ignore it
				continue
			}
			agg := a.index[series.Target]
			agg.values = append(agg.values, value)
		}

		ts, err := resp[0].Datapoints[point][1].Int64()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to parse timestamp out of series 1 point #%d: %s\n", point, err.Error())
			os.Exit(2)
		}

		for outKey, agg := range a.aggregations {
			if len(agg.values) != 0 {
				outcome := a.fn(agg.values)
				err := a.flush(outKey, outcome, ts)
				if err != nil {
					fmt.Fprintf(os.Stderr, "could not send metric:%s\n", err.Error())
				}
			}
		}
	}
}

func (a *Aggregator) flush(key string, outcome float64, ts int64) error {
	metric := carbon.NewMetric(key, fmt.Sprintf("%f", outcome), ts)
	return a.carbonClient.SendMetric(metric)
}
