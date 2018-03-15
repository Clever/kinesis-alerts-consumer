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
		Dimensions: []string{"Hostname", "env", "source"},
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
		Dimensions: []string{"Hostname", "env", "source"},
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
		Dimensions: []string{"Hostname", "env"},
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
		"env":    "production",
	}
	routes = gearmanRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "gearman.success",
		StatType:   statTypeCounter,
		Dimensions: []string{"Hostname", "function"},
		ValueField: defaultValueField,
		RuleName:   "global-gearman",
	}
	assert.Equal(t, expected, routes[0])

	t.Log("Routes a log - gearman failure")
	fields = map[string]interface{}{
		"source": "gearman",
		"title":  "failure",
		"env":    "production",
	}
	routes = gearmanRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected = decode.AlertRoute{
		Series:     "gearman.failure",
		StatType:   statTypeCounter,
		Dimensions: []string{"Hostname", "function"},
		ValueField: defaultValueField,
		RuleName:   "global-gearman",
	}
	assert.Equal(t, expected, routes[0])

	t.Log("Routes a log only if env==production")
	fields = map[string]interface{}{
		"source": "gearman",
		"title":  "success",
		"env":    "dev",
	}
	routes = gearmanRoutes(fields)
	assert.Equal(t, 0, len(routes))
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
		"env":    "production",
	}
	routes = gearcmdPassfailRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "gearcmd.passfail",
		StatType:   statTypeGauge,
		Dimensions: []string{"Hostname", "function"},
		ValueField: defaultValueField,
		RuleName:   "global-gearcmd-passfail",
	}
	assert.Equal(t, expected, routes[0])

	t.Log("Routes a log only if env==production")
	fields = map[string]interface{}{
		"source": "gearcmd",
		"title":  "END",
		"env":    "dev",
	}
	routes = gearcmdPassfailRoutes(fields)
	assert.Equal(t, 0, len(routes))
}

func TestGearcmdDurationRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := gearcmdDurationRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log - gearcmd duration")
	fields = map[string]interface{}{
		"source": "gearcmd",
		"title":  "duration",
	}
	routes = gearcmdDurationRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "gearcmd.duration",
		StatType:   statTypeGauge,
		Dimensions: []string{"Hostname", "function", "env"},
		ValueField: defaultValueField,
		RuleName:   "global-gearcmd-duration",
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
		Dimensions: []string{"Hostname", "env", "function", "job_id", "try_number", "unit"},
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

func TestAppLifecycleRoutes(t *testing.T) {
	t.Log("Base case: doesn't route empty log")
	fields := map[string]interface{}{}
	routes := appLifecycleRoutes(fields)
	assert.Equal(t, 0, len(routes))

	t.Log("Routes a log")
	fields = map[string]interface{}{
		"category": "app_lifecycle",
		"title":    "app_deploying",
	}
	routes = appLifecycleRoutes(fields)
	assert.Equal(t, 1, len(routes))
	expected := decode.AlertRoute{
		Series:     "app_lifecycle",
		StatType:   statTypeEvent,
		Dimensions: []string{"container_app", "container_env", "launched_scope", "title", "user", "version", "team"},
		RuleName:   "global-app-lifecycle",
	}
	assert.Equal(t, expected, routes[0])
}
