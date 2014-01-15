package puppetdb

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
)

type Client struct {
	BaseURL    string
	PublicKey  string
	SecretKey  string
	httpClient *http.Client
}

type EventCountJson struct {
	SubjectType string            `json:"subject-type"`
	Subject     map[string]string `json:"subject"`
	Failure     int64             `json:"failures"`
	Successes   int64             `json:"successes"`
	Noops       int64             `json:"noops"`
	Skips       int64             `json:"skips"`
}

type EventJson struct {
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

type FactJson struct {
	CertName string `json:"certname"`
	Name     string `json:"name"`
	Value    string `json:"value"`
}

type NodeJson struct {
	Name             string `json:"name"`
	Deactivated      string `json:"deactivated"`
	CatalogTimestamp string `json:"catalog_timestamp"`
	FactsTimestamp   string `json:"facts_timestamp"`
	ReportTimestamp  string `json:"report_timestamp"`
}

type ReportJson struct {
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

type ValueMetricJson struct {
	Value float64
}

func NewClient(baseUrl string) *Client {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: tr}
	return &Client{baseUrl, "", "", client}
}

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

func (c *Client) Nodes() ([]NodeJson, error) {
	ret := []NodeJson{}
	err := c.Get(&ret, "nodes", nil)
	return ret, err
}

func (c *Client) NodeFacts(node string) ([]FactJson, error) {
	url := fmt.Sprintf("nodes/%s/facts", node)
	ret := []FactJson{}
	err := c.Get(&ret, url, nil)
	return ret, err
}

func (c *Client) EventCounts(query string, summarizeBy string, extraParams map[string]string) ([]EventCountJson, error) {
	path := "event-counts"
	ret := []EventCountJson{}
	params := mergeParam("query", query, extraParams)
	params = mergeParam("summarize-by", summarizeBy, params)
	err := c.Get(&ret, path, params)
	return ret, err
}

func (c *Client) Events(query string, extraParams map[string]string) ([]EventJson, error) {
	path := "events"
	ret := []EventJson{}
	params := mergeParam("query", query, extraParams)
	err := c.Get(&ret, path, params)
	return ret, err
}

func (c *Client) Metric(v interface{}, metric string) error {
	url := fmt.Sprintf("metrics/mbean/%s", metric)
	err := c.Get(&v, url, nil)
	return err
}

func (c *Client) MetricResourcesPerNode() (result float64, err error) {
	ret := ValueMetricJson{}
	return ret.Value, c.Metric(&ret, "com.puppetlabs.puppetdb.query.population:type=default,name=avg-resources-per-node")
}

func (c *Client) MetricNumResources() (result float64, err error) {
	ret := ValueMetricJson{}
	return ret.Value, c.Metric(&ret, "com.puppetlabs.puppetdb.query.population:type=default,name=num-resources")
}

func (c *Client) MetricNumNodes() (result float64, err error) {
	ret := ValueMetricJson{}
	return ret.Value, c.Metric(&ret, "com.puppetlabs.puppetdb.query.population:type=default,name=num-nodes")
}

func (c *Client) Reports(query string, extraParams map[string]string) ([]ReportJson, error) {
	path := "reports"
	ret := []ReportJson{}
	params := mergeParam("query", query, extraParams)
	err := c.Get(&ret, path, params)
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
	url := fmt.Sprintf("%s/v3/%s", base, endpoint)
	log.Printf(url)
	return c.httpClient.Get(url)
}
