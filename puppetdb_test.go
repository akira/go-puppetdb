package puppetdb

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/Jeffail/gabs"
)

var (
	mux    *http.ServeMux
	client *Client
	server *httptest.Server
)

func setup() {
	mux = http.NewServeMux()
	server = httptest.NewServer(mux)

	serverURL, _ := url.Parse(server.URL)
	splitsy := strings.Split(serverURL.Host, ":")
	port, _ := strconv.Atoi(splitsy[1])
	client = NewClient(splitsy[0], port, true)
}

func teardown() {
	server.Close()
}

func testMethod(t *testing.T, r *http.Request, want string) {
	if want != r.Method {
		t.Errorf("Request method = %v, want %v", r.Method, want)
	}
}

func TestNodes(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/pdb/query/v4/nodes",
		func(w http.ResponseWriter, r *http.Request) {
			testMethod(t, r, "GET")
			fmt.Fprint(w, `[{"deactivated": "",
				"latest_report_hash": "somehashqsdnqosdnlq",
				"facts_environment": "development",
				"cached_catalog_status": "on_failure",
				"report_environment": "development",
				"latest_report_corrective_change": "",
				"catalog_environment": "development",
				"facts_timestamp": "2019-01-30T09:46:27.804Z",
				"latest_report_noop": false,
				"expired": "",
				"latest_report_noop_pending": false,
				"report_timestamp": "2019-01-30T09:46:31.347Z",
				"certname": "nodename",
				"catalog_timestamp": "2018-12-07T08:46:24.216Z",
				"latest_report_job_id": "",
				"latest_report_status": "unchanged"			
							 }]`)
		})

	nodes, err := client.Nodes()
	if err != nil {
		t.Errorf("Nodes() returned error: %v", err)
	}

	want := []NodeJSON{NodeJSON{"nodename", "", "2018-12-07T08:46:24.216Z",
		"2019-01-30T09:46:27.804Z", "development", "development",
		"somehashqsdnqosdnlq", "on_failure", "development",
		"2019-01-30T09:46:31.347Z", "", false,
		false, "", "", "unchanged"}}
	if !reflect.DeepEqual(nodes, want) {
		t.Errorf("Nodes() returned %+v, want %+v",
			nodes, want)
	}
}

func TestFactNames(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/pdb/query/v4/fact-names",
		func(w http.ResponseWriter, r *http.Request) {
			testMethod(t, r, "GET")
			fmt.Fprint(w, `[ "fact1", "fact2", "fact3" ]`)
		})

	facts, err := client.FactNames()
	if err != nil {
		t.Errorf("FactNames() returned error: %v", err)
	}
	want := []string{"fact1", "fact2", "fact3"}
	if !reflect.DeepEqual(facts, want) {
		t.Errorf("FactNames() returned %+v, want %+v",
			facts, want)
	}
}

func TestNodeFacts(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/pdb/query/v4/nodes/node123/facts",
		func(w http.ResponseWriter, r *http.Request) {
			testMethod(t, r, "GET")
			fmt.Fprint(w, `[{"certname" : "node123",
			  				 "name" : "uptime_seconds",
							   "value" : "9708003",
							   "environment": "production"
							}]`)
		})

	facts, err := client.NodeFacts("node123")
	if err != nil {
		t.Errorf("NodesFacts() returned error: %v", err)
	}

	jsonParsed, err := gabs.ParseJSON([]byte(`
		"9708003"

	`))
	var want = []FactJSON{FactJSON{"node123", "production", "uptime_seconds", jsonParsed}}
	if !reflect.DeepEqual(facts, want) {
		t.Errorf("NodeFacts() returned %+v, want %+v",
			facts, want)
	}
}

func TestMetricResourcesPerNode(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/pdb/query/v4/metrics/mbean/com.puppetlabs.puppetdb.query.population:type=default,name=avg-resources-per-node",
		func(w http.ResponseWriter, r *http.Request) {
			testMethod(t, r, "GET")
			fmt.Fprint(w, `{"Value" : 309.130}`)
		})

	value, err := client.MetricResourcesPerNode()
	if err != nil {
		t.Errorf("Nodes() returned error: %v", err)
	}

	want := 309.130
	if want != value {
		t.Errorf("Nodes() returned %f, want %f",
			value, want)
	}
}

func TestPuppetdbVersion(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/pdb/query/v4/version",
		func(w http.ResponseWriter, r *http.Request) {
			testMethod(t, r, "GET")
			fmt.Fprint(w, `{ "version" : "2.2.0" },`)
		})

	facts, err := client.PuppetdbVersion()
	if err != nil {
		t.Errorf("PuppetdbVersion() returned error: %v", err)
	}
	want := Version{"2.2.0"}
	if !reflect.DeepEqual(facts, want) {
		t.Errorf("PuppetdbVersion() returned %+v, want %+v",
			facts, want)
	}
}

func TestNodeReports(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/pdb/query/v4/reports",
		func(w http.ResponseWriter, r *http.Request) {
			testMethod(t, r, "GET")
			fmt.Fprint(w, `[{
				"catalog_uuid": "3c77fcff-7e95-4129-a28f-af86566db52f",
				"receive_time": "2019-02-19T13:27:21.312Z",
				"producer": "puppet",
				"hash": "ceb97994c50a968859dd44b90e14bb08e54e53ea",
				"transaction_uuid": "005d2b4a-5a89-4096-9f81-ecc65f1e9082",
				"puppet_version": "5.5.1",
				"noop": false,
				"corrective_change": null,
				"logs": {
					"data": [
						{
							"file": null,
							"line": null,
							"tags": [
								"notice"
							],
							"time": "2019-02-19T14:27:21.032+01:00",
							"level": "notice",
							"source": "Puppet",
							"message": "Applied catalog in 6.69 seconds"
						}
					],
					"href": "/pdb/query/v4/reports/ceb97994c50a968859dd44b90e14bb08e54e53ea/logs"
				},
				"report_format": 9,
				"start_time": "2019-02-19T13:27:04.740Z",
				"producer_timestamp": "2019-02-19T13:27:21.282Z",
				"cached_catalog_status": "not_used",
				"end_time": "2019-02-19T13:27:20.973Z",
				"resource_events": {
					"data": null,
					"href": "/pdb/query/v4/reports/ceb97994c50a968859dd44b90e14bb08e54e53ea/events"
				},
				"status": "unchanged",
				"configuration_version": "1550582831",
				"environment": "development",
				"code_id": null,
				"noop_pending": false,
				"certname": "node",
				"metrics": {
					"data": [
						{
							"name": "changed",
							"value": 0,
							"category": "resources"
						}
					],
					"href": "/pdb/query/v4/reports/ceb97994c50a968859dd44b90e14bb08e54e53ea/metrics"
				},
				"job_id": null
			}]`)
		})

	facts, err := client.Reports("", nil)
	if err != nil {
		t.Errorf("NodesReports() returned error: %v", err)
	}
	want := []ReportJSON{ReportJSON{
		CatalogUUID:          "3c77fcff-7e95-4129-a28f-af86566db52f",
		ReceiveTime:          "2019-02-19T13:27:21.312Z",
		Producer:             "puppet",
		Hash:                 "ceb97994c50a968859dd44b90e14bb08e54e53ea",
		TransactionUUID:      "005d2b4a-5a89-4096-9f81-ecc65f1e9082",
		PuppetVersion:        "5.5.1",
		Noop:                 false,
		CorrectiveChange:     "",
		ReportFormat:         9,
		StartTime:            "2019-02-19T13:27:04.740Z",
		ProducerTimestamp:    "2019-02-19T13:27:21.282Z",
		CachedCatalogStatus:  "not_used",
		EndTime:              "2019-02-19T13:27:20.973Z",
		Status:               "unchanged",
		ConfigurationVersion: "1550582831",
		Environment:          "development",
		CodeID:               "",
		NoopPending:          false,
		CertName:             "node",
		ResourceEvents: PuppetReportResource{
			Href: "/pdb/query/v4/reports/ceb97994c50a968859dd44b90e14bb08e54e53ea/events",
		},
		Logs: PuppetReportLog{
			Data: []PuppetReportMetricsLogEntry{
				PuppetReportMetricsLogEntry{
					NewValue: "",
					Property: "",
					File:     "",
					Line:     "",
					Tags: []string{
						"notice",
					},
					Time:    "2019-02-19T14:27:21.032+01:00",
					Level:   "notice",
					Source:  "Puppet",
					Message: "Applied catalog in 6.69 seconds",
				},
			},
			Href: "/pdb/query/v4/reports/ceb97994c50a968859dd44b90e14bb08e54e53ea/logs",
		},
		Metrics: PuppetReportMetrics{
			Href: "/pdb/query/v4/reports/ceb97994c50a968859dd44b90e14bb08e54e53ea/metrics",
			Data: []PuppetReportMetricsDataEntry{
				PuppetReportMetricsDataEntry{
					Name:     "changed",
					Value:    0,
					Category: "resources",
				},
			},
		},
	}}
	if !reflect.DeepEqual(facts, want) {
		t.Errorf("NodeReports() returned %+v, want %+v",
			facts, want)
	}
}

func TestEventCounts(t *testing.T) {
	setup()
	defer teardown()

	mux.HandleFunc("/pdb/query/v4/event-counts",
		func(w http.ResponseWriter, r *http.Request) {
			testMethod(t, r, "GET")
			fmt.Fprint(w, `[{
				"subject" : {
					"title" : "node123"
				  },
				 "subject-type" : "certname",
				 "failures" : 0,
				 "successes" : 1,
				 "noops" : 0,
				 "skips" : 0
  			}]`)
		})

	facts, err := client.EventCounts("", "certname", nil)
	if err != nil {
		t.Errorf("EventCount() returned error: %v", err)
	}
	want := []EventCountJSON{EventCountJSON{"certname", map[string]string{"title": "node123"}, 0, 1, 0, 0}}
	if !reflect.DeepEqual(facts, want) {
		t.Errorf("EventCount() returned %+v, want %+v",
			facts, want)
	}
}

func TestSimpleQuery(t *testing.T) {
	query := []string{"=", "certname", "node123"}
	want := `["=","certname","node123"]`
	jsonQuery, _ := QueryToJSON(query)
	if jsonQuery != want {
		t.Errorf("SimpleQuery() returned %+v, want %+v",
			jsonQuery, want)
	}
}

func TestNestedQuery(t *testing.T) {
	query := []interface{}{"or", []string{"=", "certname", "node123"}, []string{"=", "certname", "node321"}}
	want := `["or",["=","certname","node123"],["=","certname","node321"]]`
	jsonQuery, _ := QueryToJSON(query)
	if jsonQuery != want {
		t.Errorf("SimpleQuery() returned %+v, want %+v",
			jsonQuery, want)
	}
}
