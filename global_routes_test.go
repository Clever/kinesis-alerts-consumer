package main

import (
	"testing"

	"github.com/Clever/amazon-kinesis-client-go/decode"
	"github.com/stretchr/testify/assert"
)

func TestProcessMetricsRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := processMetricsRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log - counter")
	fields = map[string]interface{}{
		"via":    "process-metrics",
		"source": "some-source",
		"title":  "some-title",
		"value":  123,
		"type":   "counter",
	}
	routes = processMetricsRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "process-metrics.some-title",
		StatType:   statTypeCounter,
		Dimensions: []string{"source"},
		ValueField: defaultValueField,
		RuleName:   "global-process-metrics",
	}
	assert.Equal(t, expected, routes[0])

	t.Log("Routes a log - gauge")
	fields = map[string]interface{}{
		"via":    "process-metrics",
		"source": "some-source-2",
		"title":  "some-title-2",
		"value":  .35,
		"type":   "gauge",
	}
	routes = processMetricsRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected = decode.AlertRoute{
		Series:     "process-metrics.some-title-2",
		StatType:   statTypeGauge,
		Dimensions: []string{"source"},
		ValueField: defaultValueField,
		RuleName:   "global-process-metrics",
	}
	assert.Equal(t, expected, routes[0])
}

func TestRsyslogRateLimitRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := rsyslogRateLimitRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log")
	fields = map[string]interface{}{
		"programname": "prefix..rsyslog..postfix",
		"rawlog":      "prefix..imuxsock begins to drop messages..postfix",
	}
	routes = rsyslogRateLimitRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "rsyslog.rate-limit-triggered",
		StatType:   statTypeCounter,
		Dimensions: []string{},
		ValueField: defaultValueField,
		RuleName:   "global-rsyslog-rate-limit",
	}
	assert.Equal(t, expected, routes[0])
}

func TestGearmanRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := gearmanRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log - gearman success")
	fields = map[string]interface{}{
		"source": "gearman",
		"title":  "success",
	}
	routes = gearmanRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "gearman.success",
		StatType:   statTypeCounter,
		Dimensions: []string{"function"},
		ValueField: defaultValueField,
		RuleName:   "global-gearman",
	}
	assert.Equal(t, expected, routes[0])

	t.Log("Routes a log - gearman failure")
	fields = map[string]interface{}{
		"source": "gearman",
		"title":  "failure",
	}
	routes = gearmanRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected = decode.AlertRoute{
		Series:     "gearman.failure",
		StatType:   statTypeCounter,
		Dimensions: []string{"function"},
		ValueField: defaultValueField,
		RuleName:   "global-gearman",
	}
	assert.Equal(t, expected, routes[0])
}

func TestGearcmdPassfailRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := gearcmdPassfailRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log - gearcmd END")
	fields = map[string]interface{}{
		"source": "gearcmd",
		"title":  "END",
	}
	routes = gearcmdPassfailRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "gearcmd.passfail",
		StatType:   statTypeGauge,
		Dimensions: []string{"function"},
		ValueField: defaultValueField,
		RuleName:   "global-gearcmd-passfail",
	}
	assert.Equal(t, expected, routes[0])
}

func TestGearcmdHeartbeatRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := gearcmdHeartbeatRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log - gearcmd hearbeat")
	fields = map[string]interface{}{
		"source": "gearcmd",
		"title":  "heartbeat",
	}
	routes = gearcmdHeartbeatRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "gearcmd.heartbeat",
		StatType:   statTypeGauge,
		Dimensions: []string{"function", "job_id", "try_number", "unit"},
		ValueField: defaultValueField,
		RuleName:   "global-gearcmd-heartbeat",
	}
	assert.Equal(t, expected, routes[0])
}

func TestWagCircuitBreakerRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := wagCircuitBreakerRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log")
	fields = map[string]interface{}{
		"source":          "prefix..wagclient..postfix",
		"errorPercentage": float64(.13),
	}
	routes = wagCircuitBreakerRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "wag.client-circuit-breakers",
		StatType:   statTypeGauge,
		Dimensions: []string{"container_env", "container_app", "title"},
		ValueField: "errorPercentage",
		RuleName:   "global-wag-circuit-breakers",
	}
	assert.Equal(t, expected, routes[0])
}
