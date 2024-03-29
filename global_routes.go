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
		{
			Series:     fmt.Sprintf("process-metrics.%s", title),
			Dimensions: []string{"Hostname", "env", "source"},
			StatType:   statType,
			ValueField: defaultValueField,
			RuleName:   "global-process-metrics",
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
		{
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
		{
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
		{
			Series:     "rds.slow-query",
			Dimensions: []string{"env", "programname"},
			StatType:   statTypeCounter,
			ValueField: defaultValueField,
			RuleName:   "global-rds-slow-query-count",
		},
	}
}
