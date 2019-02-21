package puppetdb

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
)

// Client This represents a connection to your puppetdb instance
type Client struct {
	BaseURL    string
	Cert       string
	Key        string
	httpClient *http.Client
	verbose    bool
}

// EventCountJSON A json object holding the results of a query to the eventcount api
type EventCountJSON struct {
	SubjectType string            `json:"subject-type"`
	Subject     map[string]string `json:"subject"`
	Failure     int64             `json:"failures"`
	Successes   int64             `json:"successes"`
	Noops       int64             `json:"noops"`
	Skips       int64             `json:"skips"`
}

// EventJSON A json object holding the results of a query to the event api.
type EventJSON struct {
	CertName          string `json:"certname"`
	OldValue          string `json:"old-value"`
	Property          string `json:"property"`
	Timestamp         string `json:"timestamp"`
	ResourceType      string `json:"resource-typ"`
	ResourceTitle     string `json:"resource-title"`
	NewValue          string `json:"new-value"`
	Message           string `json:"message"`
	Report            string `json:"report"`
	Status            string `json:"status"`
	File              string `json:"file"`
	ContainmentPath   string `json:"containment-path"`
	ContainmentClass  string `json:"containing-class"`
	RunStartTime      string `json:"run-start-time"`
	RunEndTime        string `json:"run-end-time"`
	ReportReceiveTime string `json:"report-receive-time"`
}

// FactJSON A json object holding the results of a query to the facts api.
type FactJSON struct {
	CertName    string          `json:"certname"`
	Environment string          `json:"environment"`
	Name        string          `json:"name"`
	Value       *gabs.Container `json:"value"`
}

// NodeJSON A json object holding the results of query to the node api.
type NodeJSON struct {
	Certname                     string `json:"certname"`
	Deactivated                  string `json:"deactivated"`
	CatalogTimestamp             string `json:"catalog_timestamp"`
	FactsTimestamp               string `json:"facts_timestamp"`
	CatalogEnvironment           string `json:"catalog_environment"`
	FactsEnvironment             string `json:"facts_environment"`
	LatestReportHash             string `json:"latest_report_hash"`
	CachedCatalogStatus          string `json:"cached_catalog_status"`
	ReportEnvironment            string `json:"report_environment"`
	ReportTimestamp              string `json:"report_timestamp"`
	LatestReportCorrectiveChange string `json:"latest_report_corrective_change"`
	LatestReportNoop             bool   `json:"latest_report_noop"`
	LatestReportNoopPending      bool   `json:"latest_report_noop_pending"`
	Expired                      string `json:"expired"`
	LatestReportJobID            string `json:"latest_report_job_id"`
	LatestReportStatus           string `json:"latest_report_status"`
}

// Version a simple struct holding the puppetdb version.
type Version struct {
	Version string `json:"version"`
}

type PuppetReportLog struct {
	Href string                        `json:"href"`
	Data []PuppetReportMetricsLogEntry `json:"data"`
}

type PuppetReportResource struct {
	Href string `json:"href"`
}
type PuppetReportMetrics struct {
	Href string                         `json:"href"`
	Data []PuppetReportMetricsDataEntry `json:"data"`
}

type PuppetReportMetricsDataEntry struct {
	Name     string  `json:"name"`
	Value    float64 `json:"value"`
	Category string  `json:"category"`
}

type PuppetReportMetricsLogEntry struct {
	NewValue string   `json:"new_value"`
	Property string   `json:"property"`
	File     string   `json:"file"`
	Line     string   `json:"line"`
	Tags     []string `json:"tags"`
	Time     string   `json:"time"`
	Level    string   `json:"level"`
	Source   string   `json:"source"`
	Message  string   `json:"message"`
}

// ReportJSON A json abject holding the data for a query from the report api.
type ReportJSON struct {
	CertName             string               `json:"certname"`
	PuppetVersion        string               `json:"puppet_version"`
	Value                string               `json:"value"`
	Hash                 string               `json:"hash"`
	ReportFormat         int64                `json:"report_format"`
	ConfigurationVersion string               `json:"configuration_version"`
	CatalogUUID          string               `json:"catalog_uuid"`
	TransactionUUID      string               `json:"transaction_uuid"`
	StartTime            string               `json:"start_time"`
	EndTime              string               `json:"end_time"`
	ReceiveTime          string               `json:"receive_time"`
	Noop                 bool                 `json:"noop"`
	Producer             string               `json:"producer"`
	CorrectiveChange     string               `json:"corrective_change"`
	Logs                 PuppetReportLog      `json:"logs"`
	ProducerTimestamp    string               `json:"producer_timestamp"`
	CachedCatalogStatus  string               `json:"cached_catalog_status"`
	ResourceEvents       PuppetReportResource `json:"resource_events"`
	Status               string               `json:"Status"`
	Environment          string               `json:"Environment"`
	CodeID               string               `json:"code_id"`
	NoopPending          bool                 `json:"noop_pending"`
	Metrics              PuppetReportMetrics  `json:"metrics"`
}

//Resource contains information about a puppet resource.
type Resource struct {
	Paramaters map[string]interface{} `json:"parameters"`
	Line       int                    `json:"line,omitempty"`
	Exported   bool                   `json:"exported,omitempty"`
	Tags       []string               `json:"tags,omitempty"`
	Title      string                 `json:"title,omitempty"`
	Type       string                 `json:"type,omitempty"`
	Resource   string                 `json:"resource,omitempty"`
	Certname   string                 `json:"certname,omitempty"`
}

// ValueMetricJSON A simple structholding a float value.
type ValueMetricJSON struct {
	Value float64
}

// getURL return the address of the puppetdb instance.
func getURL(host string, port int, ssl bool) string {
	if ssl {
		return fmt.Sprintf("https://%s:%v", host, port)
	} else {
		return fmt.Sprintf("http://%s:%v", host, port)
	}
}

// NewClient returns a http connection for your puppetdb instance.
func NewClient(host string, port int, verbose bool) *Client {

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}
	return &Client{getURL(host, port, false), "", "", client, verbose}
}

// NewClientSSL returns a https connection for your puppetdb instance.
func NewClientSSL(host string, port int, key string, cert string, ca string, verbose bool) *Client {
	flag.Parse()
	cert2, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		log.Fatal(err)
	}
	// Load CA cert
	caCert, err := ioutil.ReadFile(ca)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert2},
		RootCAs:      caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}
	return &Client{getURL(host, port, true), cert, key, client, verbose}

}

// NewClientTimeout returns a http connection for your puppetdb instance with a timeout.
func NewClientTimeout(host string, port int, verbose bool, timeout int) *Client {

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr, Timeout: time.Duration(timeout) * time.Second}
	return &Client{getURL(host, port, false), "", "", client, verbose}
}

// NewClientTimeoutSSL returns a http connection for your puppetdb instance with a timeout and ssl configured.
func NewClientTimeoutSSL(host string, port int, key string, cert string, ca string, verbose bool, timeout int) *Client {
	flag.Parse()
	cert2, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		log.Fatal(err)
	}
	// Load CA cert
	caCert, err := ioutil.ReadFile(ca)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert2},
		RootCAs:      caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport, Timeout: time.Duration(timeout) * time.Second}
	return &Client{getURL(host, port, true), cert, key, client, verbose}

}

// Get gets the given url and retruns the result. In form of the given interface.
func (c *Client) Get(v interface{}, path string, params map[string]string) error {
	pathAndParams := path
	//TODO: Improve this
	if params != nil && len(params) > 0 {
		if !strings.Contains(path, "?") {
			pathAndParams += "?"
		}
		for k, v := range params {
			pathAndParams += fmt.Sprintf("%s=%s&", k, url.QueryEscape(v))
		}
	}
	resp, err := c.httpGet(pathAndParams)
	if err != nil {
		log.Print(err)
		return err
	}
	defer resp.Body.Close()
	if err != nil {
		log.Print(err)
		return err
	}
	json.NewDecoder(resp.Body).Decode(&v)
	return err
}

// GetFacts returns an array of Json facts and returns them. It now uses gabs array because json value is not consistent.
func (c *Client) GetFacts(path string) ([]FactJSON, error) {
	pathAndParams := path
	ret := []FactJSON{}
	resp, err := c.httpGet(pathAndParams)
	if err != nil {
		log.Print(err)
		return ret, err
	}
	defer resp.Body.Close()
	if err != nil {
		log.Print(err)
		return ret, err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	jsonParsed, _ := gabs.ParseJSON(body)
	count, err := jsonParsed.ArrayCount()
	counter := 0
	for counter < count {
		something, _ := jsonParsed.ArrayElement(counter)
		certname := something.Path("certname").Data().(string)
		value := something.Path("value")
		j := FactJSON{
			certname,
			something.Path("environment").Data().(string),
			something.Path("name").Data().(string),
			value,
		}
		ret = append(ret, j)
		//c := value.Data()
		//fmt.Println(reflect.TypeOf(c))
		//println(c.(string)
		counter++
	}
	return ret, err

}

// Nodes Polls the nodes api of your puppetdb and returns the results in form of the NodeJSON type.
func (c *Client) Nodes() ([]NodeJSON, error) {
	ret := []NodeJSON{}
	err := c.Get(&ret, "nodes", nil)
	return ret, err
}

// FactNames Gets all the fact names
func (c *Client) FactNames() ([]string, error) {
	ret := []string{}
	err := c.Get(&ret, "fact-names", nil)
	return ret, err
}

// NodeFacts Gets all the facts for a specified node.
func (c *Client) NodeFacts(node string) ([]FactJSON, error) {
	PUrl := fmt.Sprintf("nodes/%s/facts", node)
	ret, err := c.GetFacts(PUrl)
	return ret, err
}

// FactPerNode Gets all nodes values for a specified fact.
func (c *Client) FactPerNode(fact string) ([]FactJSON, error) {
	PUrl := fmt.Sprintf("facts/%s", fact)
	ret, err := c.GetFacts(PUrl)
	return ret, err
}

// EventCounts Returns the even counts
func (c *Client) EventCounts(query string, summarizeBy string, extraParams map[string]string) ([]EventCountJSON, error) {
	path := "event-counts"
	ret := []EventCountJSON{}
	params := mergeParam("query", query, extraParams)
	params = mergeParam("summarize-by", summarizeBy, params)
	err := c.Get(&ret, path, params)
	return ret, err
}

// Events returns the events
func (c *Client) Events(query string, extraParams map[string]string) ([]EventJSON, error) {
	path := "events"
	ret := []EventJSON{}
	params := mergeParam("query", query, extraParams)
	err := c.Get(&ret, path, params)
	return ret, err
}

//Resources will fetch resources from /resources/ in the puppetdb api
func (c *Client) Resources(query string, extraParams map[string]string) ([]Resource, error) {
	in := []Resource{}
	params := mergeParam("query", query, extraParams)
	err := c.Get(&in, "resources", params)
	return in, err
}

// Metric returns a metric
func (c *Client) Metric(v interface{}, metric string) error {
	PUrl := fmt.Sprintf("metrics/mbean/%s", metric)
	err := c.Get(&v, PUrl, nil)
	return err
}

// MetricResourcesPerNode Gets the specified metric per node.
func (c *Client) MetricResourcesPerNode() (result float64, err error) {
	ret := ValueMetricJSON{}
	return ret.Value, c.Metric(&ret, "com.puppetlabs.puppetdb.query.population:type=default,name=avg-resources-per-node")
}

func (c *Client) MetricNumResources() (result float64, err error) {
	ret := ValueMetricJSON{}
	return ret.Value, c.Metric(&ret, "com.puppetlabs.puppetdb.query.population:type=default,name=num-resources")
}

func (c *Client) MetricNumNodes() (result float64, err error) {
	ret := ValueMetricJSON{}
	return ret.Value, c.Metric(&ret, "com.puppetlabs.puppetdb.query.population:type=default,name=num-nodes")
}

// Reports Gets the reports with the specified querry.
func (c *Client) Reports(query string, extraParams map[string]string) ([]ReportJSON, error) {
	path := "reports"
	ret := []ReportJSON{}
	params := mergeParam("query", query, extraParams)
	err := c.Get(&ret, path, params)
	return ret, err
}

// ReportByHash Gets the report for this specific hash
func (c *Client) ReportByHash(hash string) ([]ReportJSON, error) {
	path := fmt.Sprintf("reports")
	ret := []ReportJSON{}
	q := fmt.Sprintf("[\"=\", \"hash\", \"%s\"]", hash)
	params := mergeParam("query", q, nil)

	err := c.Get(&ret, path, params)
	return ret, err
}

// PuppetdbVersion gets the specified puppetdb version.
func (c *Client) PuppetdbVersion() (Version, error) {
	path := "version"
	ret := Version{}
	err := c.Get(&ret, path, nil)
	return ret, err
}

// QueryToJSON Converts a query to json.
func QueryToJSON(query interface{}) (result string, err error) {
	resultBytes, err := json.Marshal(query)
	jsonQuery := string(resultBytes[:])
	return jsonQuery, err
}

func mergeParam(paramName string, paramValue string, params map[string]string) map[string]string {
	resultParams := make(map[string]string)
	if paramValue != "" {
		resultParams[paramName] = paramValue
	}
	if params != nil && len(params) > 0 {
		for k, v := range params {
			resultParams[k] = v
		}
	}
	return resultParams
}

func (c *Client) httpGet(endpoint string) (resp *http.Response, err error) {
	base := strings.TrimRight(c.BaseURL, "/")
	PUrl := fmt.Sprintf("%s/pdb/query/v4/%s", base, endpoint)
	if c.verbose == true {
		log.Printf(PUrl)
	}
	return c.httpClient.Get(PUrl)
}
