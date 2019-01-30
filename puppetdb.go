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
	Property          string `json:"message"`
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
	CertName    string `json:"certname"`
	Environment string `json:"environment"`
	Name        string `json:"name"`
	Value       string `json:"value"`
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

// PuppetdbVersion a simple struct holding the puppetdb version.
type PuppetdbVersion struct {
	Version string `json:"version"`
}

// ReportJSON A json abject holding the data for a query from the report api.
type ReportJSON struct {
	CertName             string `json:"certname"`
	PuppetVersion        string `json:"puppet-version"`
	Value                string `json:"value"`
	Hash                 string `json:"hash"`
	ReportFormat         int64  `json:"report-format"`
	ConfigurationVersion string `json:"configuration-version"`
	TransactionUUID      string `json:"transaction-uuid"`
	StartTime            string `json:"start-time"`
	EndTime              string `json:"end-time"`
	ReceiveTime          string `json:"receive-time"`
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
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert2},
		RootCAs:            caCertPool,
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
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert2},
		RootCAs:            caCertPool,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport, Timeout: time.Duration(timeout) * time.Second}
	return &Client{getURL(host, port, true), cert, key, client, verbose}

}

// Get gets the given url and retrusn the result. In form of the given interface.
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

// Nodes Polls the nodes api of your puppetdb and returns the results in form of the NodeJSON type.
func (c *Client) Nodes() ([]NodeJSON, error) {
	ret := []NodeJSON{}
	err := c.Get(&ret, "nodes", nil)
	return ret, err
}

func (c *Client) FactNames() ([]string, error) {
	ret := []string{}
	err := c.Get(&ret, "fact-names", nil)
	return ret, err
}

func (c *Client) NodeFacts(node string) ([]FactJSON, error) {
	pUrl := fmt.Sprintf("nodes/%s/facts", node)
	ret := []FactJSON{}
	err := c.Get(&ret, pUrl, nil)
	return ret, err
}

func (c *Client) FactPerNode(fact string) ([]FactJSON, error) {
	pUrl := fmt.Sprintf("facts/%s", fact)
	ret := []FactJSON{}
	err := c.Get(&ret, pUrl, nil)
	return ret, err
}

func (c *Client) EventCounts(query string, summarizeBy string, extraParams map[string]string) ([]EventCountJSON, error) {
	path := "event-counts"
	ret := []EventCountJSON{}
	params := mergeParam("query", query, extraParams)
	params = mergeParam("summarize-by", summarizeBy, params)
	err := c.Get(&ret, path, params)
	return ret, err
}

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

func (c *Client) Metric(v interface{}, metric string) error {
	pUrl := fmt.Sprintf("metrics/mbean/%s", metric)
	err := c.Get(&v, pUrl, nil)
	return err
}

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

func (c *Client) Reports(query string, extraParams map[string]string) ([]ReportJSON, error) {
	path := "reports"
	ret := []ReportJSON{}
	params := mergeParam("query", query, extraParams)
	err := c.Get(&ret, path, params)
	return ret, err
}

func (c *Client) PuppetdbVersion() (PuppetdbVersion, error) {
	path := "version"
	ret := PuppetdbVersion{}
	err := c.Get(&ret, path, nil)
	return ret, err
}

func QueryToJson(query interface{}) (result string, err error) {
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
	pUrl := fmt.Sprintf("%s/pdb/query/v4/%s", base, endpoint)
	if c.verbose == true {
		log.Printf(pUrl)
	}
	return c.httpClient.Get(pUrl)
}
