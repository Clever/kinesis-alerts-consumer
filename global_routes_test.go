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

func TestMongoSlowQueries(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		rawlog      string
		operation   string
		namespace   string
		is_collscan bool
		millis      float64
		isNotMatch  bool
	}{
		{
			rawlog:      `[conn2852884] update clever.students query: { district: ObjectId('527bac1858c5a34a0c0000d0'), _id: ObjectId('598894d5d6528a4c00036450') } update: { $set: { location: { zip: "", state: "", address: "", city: "" } }, $unset: { enrollments: true, _rti_status: true, rti_ela: true, rti_social: true, rti_math: true, rti_behavior: true, rti_health: true, rti_communication: true, rti_gifted: true, _iep_code: true, _rti_code: true, _emails: true } } nscanned:1 nscannedObjects:1 nMatched:1 nModified:1 keyUpdates:0 writeConflicts:0 numYields:1 locks:{ Global: { acquireCount: { r: 3, w: 3 } }, Database: { acquireCount: { w: 3 }, acquireWaitCount: { w: 1 }, timeAcquiringMicros: { w: 4234 } }, Collection: { acquireCount: { w: 2 } }, oplog: { acquireCount: { w: 1 } } } 2964ms`,
			operation:   `update`,
			namespace:   `clever.students`,
			millis:      2964,
			is_collscan: false,
		},
		{
			rawlog:      `[conn2852884] command clever.$cmd command: update { update: "students", updates: [ { q: { district: ObjectId('527bac1858c5a34a0c0000d0'), _id: ObjectId('57a2331e57318b235f03f171') }, u: { $set: { location: { state: "", address: "", zip: "", city: "" } }, $unset: { _rti_code: true, enrollments: true, rti_ela: true, rti_health: true, rti_behavior: true, _rti_status: true, _iep_code: true, rti_math: true, _emails: true, rti_communication: true, rti_gifted: true, rti_social: true } }, upsert: true } ], writeConcern: { getLastError: 1 }, ordered: true } keyUpdates:0 writeConflicts:0 numYields:0 reslen:95 locks:{ Global: { acquireCount: { r: 4, w: 4 } }, Database: { acquireCount: { w: 4 } }, Collection: { acquireCount: { w: 3 } }, Metadata: { acquireCount: { W: 1 } }, oplog: { acquireCount: { w: 1 } } } 4608ms`,
			operation:   `command`,
			namespace:   `clever.$cmd`,
			millis:      4608,
			is_collscan: false,
		},
		{
			rawlog:      `[conn2852884] update clever.students query: { district: ObjectId('527bac1858c5a34a0c0000d0'), _id: ObjectId('57a2331e57318b235f03f171') } update: { $set: { location: { state: "", address: "", zip: "", city: "" } }, $unset: { _rti_code: true, enrollments: true, rti_ela: true, rti_health: true, rti_behavior: true, _rti_status: true, _iep_code: true, rti_math: true, _emails: true, rti_communication: true, rti_gifted: true, rti_social: true } } nscanned:1 nscannedObjects:1 nMatched:1 nModified:1 keyUpdates:0 writeConflicts:0 numYields:1 locks:{ Global: { acquireCount: { r: 3, w: 3 } }, Database: { acquireCount: { w: 3 } }, Collection: { acquireCount: { w: 2 } }, oplog: { acquireCount: { w: 1 } } } 4608ms`,
			operation:   `update`,
			namespace:   `clever.students`,
			millis:      4608,
			is_collscan: false,
		},
		{
			rawlog:      `[conn5261282] command archive.archive.sections command: getMore { getMore: 136494780397, collection: "archive.sections" } originatingCommand: { find: "archive.sections", filter: { _id: { $regex: /^53daa05528c680240d001ea2..+/ } }, skip: 0 } planSummary: IXSCAN { _id: 1 } cursorid:136494780397 keysExamined:43401 docsExamined:43400 cursorExhausted:1 numYields:340 nreturned:43400 reslen:4589709 locks:{ Global: { acquireCount: { r: 682 } }, Database: { acquireCount: { r: 341 } }, Collection: { acquireCount: { r: 341 } } } protocol:op_query 112ms`,
			operation:   `command`,
			namespace:   `archive.archive.sections`,
			millis:      112,
			is_collscan: false,
		},
		{
			rawlog:      `[conn18124] remove clever.studentcontacts query: { district: ObjectId('5a15d3f286c90f00017376ef'), _id: ObjectId('5a15d5f70c3828572b00001d') } ndeleted:1 keyUpdates:0 writeConflicts:0 numYields:1 locks:{ Global: { acquireCount: { r: 3, w: 3 } }, Database: { acquireCount: { w: 3 }, acquireWaitCount: { w: 1 }, timeAcquiringMicros: { w: 5597 } }, Collection: { acquireCount: { w: 2 } }, oplog: { acquireCount: { w: 1 } } } 11906ms`,
			operation:   `remove`,
			namespace:   `clever.studentcontacts`,
			millis:      11906,
			is_collscan: false,
		},
		{
			rawlog:      `[conn1990136] getmore local.oplog.rs query: { ts: { $gte: Timestamp 1533635999000|220 } } cursorid:338612476018 ntoreturn:0 keyUpdates:0 writeConflicts:0 numYields:8 nreturned:1144 reslen:181853 locks:{ Global: { acquireCount: { r: 20 }, acquireWaitCount: { r: 1 }, timeAcquiringMicros: { r: 2743564 } }, Database: { acquireCount: { r: 10 } }, oplog: { acquireCount: { r: 10 } } } 3747ms`,
			operation:   `getmore`,
			namespace:   `local.oplog.rs`,
			millis:      3747,
			is_collscan: false,
		},
		{
			rawlog:      `[conn2838422] query clever.students query: { orderby: { name: 1, _id: 1 }, $maxTimeMS: 10000, $query: { district: ObjectId('51e5622080da6210550053a4') } } planSummary: IXSCAN { district: 1.0, _id: 1.0 }, IXSCAN { district: 1.0, _id: 1.0 } cursorid:303158689425 ntoreturn:100 ntoskip:0 nscanned:320707 nscannedObjects:320707 scanAndOrder:1 keyUpdates:0 writeConflicts:0 numYields:2506 nreturned:100 reslen:75755 locks:{ Global: { acquireCount: { r: 5014 } }, Database: { acquireCount: { r: 2507 } }, Collection: { acquireCount: { r: 2507 } } } 1729ms`,
			operation:   `query`,
			namespace:   `clever.students`,
			millis:      1729,
			is_collscan: false,
		},
		{
			rawlog:      `[conn21710592] insert instant-login.users query: { _id: ObjectId('5b68f10096a71402d9f69e53'), district: ObjectId('51e76ab1d93412f47b000c32'), type: "student", user_id: ObjectId('5b68d36caad954131dbbfa7b'), username: "", normalized_username: "", email: "jvillalo0125@mymail.lausd.net", password: "", cannot_change_password: false, sis_id: "200033x436", credentials: { district_username: "" }, state_id: "8785787005", staff_id: "", student_number: "200033x436", teacher_number: "", emails: [ "jvillalo0125@mymail.lausd.net" ], disabled: false, created: new Date(1533604096317), school_id: ObjectId('598b2575e916edfd5600076f'), grade: "Other", ell_status: false, iep_status: false } ninserted:1 keyUpdates:0 writeConflicts:0 numYields:0 locks:{ Global: { acquireCount: { r: 2, w: 2 } }, Database: { acquireCount: { w: 2 } }, Collection: { acquireCount: { w: 1 } }, oplog: { acquireCount: { w: 1 } } } 271ms`,
			operation:   `insert`,
			namespace:   `instant-login.users`,
			millis:      271,
			is_collscan: false,
		},
		{
			rawlog:      `[conn20887805] query business-data.mauhistory query: { clever_id: ObjectId('58c83465cc56680001d02a76') } planSummary: COLLSCAN ntoskip:0 nscanned:0 nscannedObjects:9979 keyUpdates:0 writeConflicts:0 numYields:77 nreturned:1 reslen:24941 locks:{ Global: { acquireCount: { r: 156 } }, Database: { acquireCount: { r: 78 } }, Collection: { acquireCount: { r: 78 } } } 168ms`,
			operation:   `query`,
			namespace:   `business-data.mauhistory`,
			millis:      168,
			is_collscan: true,
		},
		{
			rawlog:     "hello hello hello hello hello hello hello hello hello hello hello hello",
			isNotMatch: true,
		},
	}

	for _, test := range tests {
		t.Log(test.rawlog[:50] + "...")

		fields := map[string]interface{}{"rawlog": test.rawlog}
		routes := mongoSlowQueries(&fields)

		if test.isNotMatch {
			assert.Len(routes, 0)
			assert.Len(fields, 1)
			continue
		}

		assert.Len(routes, 2)
		assert.Len(fields, 5)

		expectedDims := []string{"hostname", "operation", "namespace", "is_collscan"}

		assert.Equal("global-mongo-slow-query-count", routes[0].RuleName)
		assert.Equal("mongo.slow-query", routes[0].Series)
		assert.Equal(expectedDims, routes[0].Dimensions)
		assert.Equal(statTypeCounter, routes[0].StatType)
		assert.Equal("", routes[0].ValueField)

		assert.Equal("global-mongo-slow-query-gauge", routes[1].RuleName)
		assert.Equal("mongo.slow-query-millis", routes[1].Series)
		assert.Equal(expectedDims, routes[1].Dimensions)
		assert.Equal(statTypeGauge, routes[1].StatType)
		assert.Equal("millis", routes[1].ValueField)

		assert.Equal(test.operation, fields["operation"])
		assert.Equal(test.namespace, fields["namespace"])
		assert.Equal(test.millis, fields["millis"])
		assert.Equal(test.is_collscan, fields["is_collscan"])
	}
}
