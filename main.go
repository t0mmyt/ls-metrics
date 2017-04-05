package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	log "github.com/Sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	buflen    = 10240
	namespace = "logstash."
)

var (
	statsdAddr = kingpin.Flag("statsd", "Host:Port of Datadog Statsd agent").Required().String()
	lsURL      = kingpin.Flag("lsurl", "Logstash HTTP API endpoint").Default("http://127.0.0.1:9600").URL()
	interval   = kingpin.Flag("interval", "Gap between metric probes").Default("10s").Duration()
	debug      = kingpin.Flag("debug", "Enable debugging").Short('d').Bool()
)

type EventsOut struct {
	Pipeline struct {
		Events struct {
			Out float64 `json:"out"`
		} `json:"events"`
	} `json:"pipeline"`
}

func GetEvents(URL url.URL) (*EventsOut, error) {
	var e EventsOut
	URL.Path = path.Join(URL.Path, "_node/stats/pipeline")
	s := URL.String()
	log.Debugf("Pulling node stats from: %s", s)
	resp, err := http.Get(s)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	json.Unmarshal(body, &e)
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func main() {
	kingpin.Parse()
	if *debug {
		log.SetLevel(log.DebugLevel)
		log.Debugln("Debug logging enabled")
	} else {
		log.SetLevel(log.InfoLevel)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Could not get hostname: %s", err)
	}
	tags := []string{fmt.Sprintf("nodename:%s", hostname)}
	// Statds Client
	log.Infof("Starting a buffered statsd client at: %s", *statsdAddr)
	c, err := statsd.NewBuffered(*statsdAddr, buflen)
	if err != nil {
		log.Fatalf("Error starting statsd client: %s", err)
	}
	c.Namespace = namespace
	// Ticker
	var dt = interval.Seconds()
	ticker := time.Tick(*interval)
	var lastCount, currCount float64

	for {
		currVals, err := GetEvents(**lsURL)
		if err != nil {
			log.Errorf("Error getting event stats: %s", err)
			<-ticker
			continue
		}

		currCount = currVals.Pipeline.Events.Out
		if lastCount == 0 {
			lastCount = currCount
			<-ticker
			continue
		}

		dy := currCount - lastCount
		rate := dy / dt

		lastCount = currCount
		select {
		case _ = <-ticker:
			// Our only valid source of time is the tick, if the processing takes longer
			// than the tick then the value is invalid
			log.Warn("Tick happened before rate could be calculated, discarding value")
		default:
			log.Debugf("Emitting rate: %.3f, with tags:%+v", rate, tags)
			c.Gauge("RateOut", float64(rate), tags, 1)
		}
		<-ticker
	}
}
