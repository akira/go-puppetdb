package puppetdb

import (
	"fmt"
	"github.com/Jeffail/gabs"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
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
			  "certname" : "node123",
			  "hash" : "abcdefg",
			  "report-format" : 3,
			  "start-time" : "2013-12-30T19:15:05.314Z",
			  "end-time" : "2013-12-30T19:15:51.521Z",
			  "puppet-version" : "3.2.4-1",
			  "configuration-version" : "1388423716",
			  "transaction-uuid" : null,
			  "receive-time" : "2013-12-30T19:16:14.911Z"
			}]`)
		})

	facts, err := client.Reports("", nil)
	if err != nil {
		t.Errorf("NodesReports() returned error: %v", err)
	}
	want := []ReportJSON{ReportJSON{"node123", "3.2.4-1", "", "abcdefg", 3, "1388423716", "",
		"2013-12-30T19:15:05.314Z", "2013-12-30T19:15:51.521Z", "2013-12-30T19:16:14.911Z"}}
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
