package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Clever/amazon-kinesis-client-go/decode"
)

const defaultValueField = "value"
const statTypeCounter = "counter"
const statTypeGauge = "gauge"
const statTypeEvent = "event"

func globalRoutes(fields map[string]interface{}) []decode.AlertRoute {
	routes := []decode.AlertRoute{}

	// chain and append all global routes here
	// TODO: After initial migration, revisit these routes and ensure they all
	// emit hostname+env via default dimensions
	routes = append(routes, processMetricsRoutes(fields)...)
	routes = append(routes, rsyslogRateLimitRoutes(fields)...)
	routes = append(routes, gearmanRoutes(fields)...)
	routes = append(routes, gearcmdPassfailRoutes(fields)...)
	routes = append(routes, gearcmdDurationRoutes(fields)...)
	routes = append(routes, gearcmdHeartbeatRoutes(fields)...)
	routes = append(routes, wagCircuitBreakerRoutes(fields)...)
	routes = append(routes, appLifecycleRoutes(fields)...)
	routes = append(routes, rdsSlowQueries(fields)...)

	return routes
}

// globalRoutesWithCustomFields some global routes inject custom dimensions by adding values to the
// fields map
func globalRoutesWithCustomFields(fields *map[string]interface{}) []decode.AlertRoute {
	routes := []decode.AlertRoute{}

	routes = append(routes, mongoSlowQueries(fields)...)

	return routes
}

// Metrics emitted by node-process-metrics and go-process-metrics libraries
func processMetricsRoutes(fields map[string]interface{}) []decode.AlertRoute {
	via, ok := fields["via"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if via != "process-metrics" {
		return []decode.AlertRoute{}
	}

	_, ok = fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	statType, ok := fields["type"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if statType == "guage" {
		statType = "gauge" // Here to fix typo in library
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     fmt.Sprintf("process-metrics.%s", title),
			Dimensions: []string{"Hostname", "env", "source"},
			StatType:   statType,
			ValueField: defaultValueField,
			RuleName:   "global-process-metrics",
		},
	}
}

// This log serves as a regression test, in case we later mistakenly re-enable RSyslog rate limiting.
func rsyslogRateLimitRoutes(fields map[string]interface{}) []decode.AlertRoute {
	programname, ok := fields["programname"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if !strings.Contains(programname, "rsyslog") {
		return []decode.AlertRoute{}
	}

	rawlog, ok := fields["rawlog"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}

	if !strings.Contains(rawlog, "imuxsock begins to drop messages") {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "rsyslog.rate-limit-triggered",
			Dimensions: []string{"Hostname", "env"},
			StatType:   statTypeCounter,
			ValueField: defaultValueField,
			RuleName:   "global-rsyslog-rate-limit",
		},
	}
}

// Legacy logs for gearman workers
func gearmanRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearman" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if !(title == "success" || title == "failure") {
		return []decode.AlertRoute{}
	}

	env, ok := fields["env"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if env != "production" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     fmt.Sprintf("gearman.%s", title),
			Dimensions: []string{"Hostname", "function"},
			StatType:   statTypeCounter,
			ValueField: defaultValueField,
			RuleName:   "global-gearman",
		},
	}
}

// Track failure rates for Gearcmd based workers
func gearcmdPassfailRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearcmd" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if title != "END" {
		return []decode.AlertRoute{}
	}

	env, ok := fields["env"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if env != "production" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "gearcmd.passfail",
			Dimensions: []string{"Hostname", "function"},
			StatType:   statTypeGauge,
			ValueField: defaultValueField,
			RuleName:   "global-gearcmd-passfail",
		},
	}
}

// Track duration for Gearcmd based workers
func gearcmdDurationRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearcmd" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if title != "duration" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "gearcmd.duration",
			Dimensions: []string{"Hostname", "function", "env"},
			StatType:   statTypeGauge,
			ValueField: defaultValueField,
			RuleName:   "global-gearcmd-duration",
		},
	}
}

// Gearman workers pipe heartbeats, to track job execution time
func gearcmdHeartbeatRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if source != "gearcmd" {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if title != "heartbeat" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "gearcmd.heartbeat",
			Dimensions: []string{"Hostname", "env", "function", "job_id", "try_number", "unit"},
			StatType:   statTypeGauge,
			ValueField: defaultValueField,
			RuleName:   "global-gearcmd-heartbeat",
		},
	}
}

// Wag client circuit breaker error rates
func wagCircuitBreakerRoutes(fields map[string]interface{}) []decode.AlertRoute {
	source, ok := fields["source"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if !strings.Contains(source, "wagclient") {
		return []decode.AlertRoute{}
	}

	_, ok = fields["errorPercentage"].(float64)
	if !ok {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "wag.client-circuit-breakers",
			Dimensions: []string{"container_env", "container_app", "title"},
			StatType:   statTypeGauge,
			ValueField: "errorPercentage",
			RuleName:   "global-wag-circuit-breakers",
		},
	}
}

var allowedLifecycleEvents = []string{
	"app_deploying",
	"app_scaling",
	"app_rollback",
	"app_stopping",
	//"app_freezing",
	//"app_unfreezing",
	"app_restarting",
	"app_canary",
	"app_canaryscaling",
}

// App Lifecycle routes
func appLifecycleRoutes(fields map[string]interface{}) []decode.AlertRoute {
	category, ok := fields["category"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}

	title, ok := fields["title"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}

	if category != "app_lifecycle" || !listContains(allowedLifecycleEvents, title) {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series: "app_lifecycle",
			Dimensions: []string{
				"container_app",
				"container_env",
				"launched_scope",
				"title",
				"user",
				"version",
				"team",
			},
			StatType: statTypeEvent,
			RuleName: "global-app-lifecycle",
		},
	}
}

func listContains(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

var reMongoSlowQuery = regexp.MustCompile(`^\[conn\d+\]\s([a-z]+)\s([^\s]+?)\s.*\s(\d+)ms$`)

func mongoSlowQueries(fields *map[string]interface{}) []decode.AlertRoute {
	rawlog, ok := (*fields)["rawlog"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}

	matches := reMongoSlowQuery.FindStringSubmatch(rawlog)
	if len(matches) < 4 {
		return []decode.AlertRoute{}
	}

	millis, err := strconv.ParseFloat(matches[3], 64)
	if err != nil {
		return []decode.AlertRoute{}
	}

	(*fields)["operation"] = matches[1]
	(*fields)["namespace"] = matches[2]
	(*fields)["is_collscan"] = strings.Contains(rawlog, "COLLSCAN")
	(*fields)["millis"] = millis

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series: "mongo.slow-query",
			Dimensions: []string{
				"hostname",
				"operation",
				"namespace",
				"is_collscan",
			},
			StatType: statTypeCounter,
			RuleName: "global-mongo-slow-query-count",
		},
		decode.AlertRoute{
			Series: "mongo.slow-query-millis",
			Dimensions: []string{
				"hostname",
				"operation",
				"namespace",
				"is_collscan",
			},
			StatType:   statTypeGauge,
			ValueField: "millis",
			RuleName:   "global-mongo-slow-query-gauge",
		},
	}
}

func rdsSlowQueries(fields map[string]interface{}) []decode.AlertRoute {
	hostname, ok := fields["hostname"].(string)
	if !ok {
		return []decode.AlertRoute{}
	}
	if hostname != "aws-rds" {
		return []decode.AlertRoute{}
	}

	// filter out slowqueries by rdsadmin
	user, ok := fields["user"].(string)
	if !ok || user == "rdsadmin[rdsadmin]" {
		return []decode.AlertRoute{}
	}

	return []decode.AlertRoute{
		decode.AlertRoute{
			Series:     "rds.slow-query",
			Dimensions: []string{"env", "programname"},
			StatType:   statTypeCounter,
			ValueField: defaultValueField,
			RuleName:   "global-rds-slow-query-count",
		},
	}
}
