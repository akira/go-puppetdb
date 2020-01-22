package puppetdb

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type ClientMaster struct {
	BaseURL    string
	Cert       string
	Key        string
	httpClient *http.Client
	verbose    bool
}

// Profiler is a struct that holds the profiler metrics for the puppet master
type Profiler struct {
	Version      string          `json:"service_version"`
	StateVersion int             `json:"service_status_version"`
	DetailLevel  string          `json:"detail_level"`
	State        string          `json:"state"`
	Status       *ProfilerStatus `json:"status"`
}

// PuppetCertificate is a struct that holds data for a puppet certificate entry
type PuppetCertificate struct {
	Name            string                       `json:"name"`
	State           string                       `json:"state"`
	DNSAltNames     []string                     `json:"dns_alt_names"`
	SubjectAltNames []string                     `json:"subject_alt_names"`
	Fingerprint     string                       `json:"fingerprint"`
	Fingerprints    PuppetCertificateFingerPrint `json:"fingerprints"`
}

// PuppetCertificateFingerprint is a struct that holds data for a puppet certificate entry's fingerprint
type PuppetCertificateFingerPrint struct {
	SHA1    string `json:"SHA1"`
	SHA256  string `json:"SHA256"`
	SHA512  string `json:"SHA512"`
	Default string `json:"default"`
}

// PuppetCertificateState is a struct that holds data for a puppet certificate state
type PuppetCertificateState struct {
	DesiredState string `json:"desired_state"`
}

// ProfilerStatus is a struct that the experimental json which holds the correct arrays
type ProfilerStatus struct {
	Experimental *ProfilerExperimental `json:"experimental"`
}

// ProfilerExperimental is a struct that holds the array of metrics
type ProfilerExperimental struct {
	FunctionMetrics *[]ProfilerFunctionMetric `json:"function-metrics"`
	ResourceMetrics *[]ProfilerResourceMetric `json:"resource-metrics"`
	CatalogMetrics  *[]ProfilerCatalogMetric  `json:"catalog-metrics"`
	PuppetdbMetrics *[]ProfilerCatalogMetric  `json:"puppetdb-metrics"`
}

// ProfilerFunctionMetric is a struct that holds the data for a single profiler function metric
type ProfilerFunctionMetric struct {
	Function  string `json:"function"`
	Count     int    `json:"count"`
	Mean      int    `json:"mean"`
	Aggregate int    `json:"aggregate"`
}

// ProfilerFunctionMetric is a struct that holds the data for a single profiler catalog metric
type ProfilerResourceMetric struct {
	Resource  string `json:"resource"`
	Count     int    `json:"count"`
	Mean      int    `json:"mean"`
	Aggregate int    `json:"aggregate"`
}

// ProfilerCatalogMetric is a struct that holds the data for a single profiler catalog metric
type ProfilerCatalogMetric struct {
	Metric    string `json:"metric"`
	Count     int    `json:"count"`
	Mean      int    `json:"mean"`
	Aggregate int    `json:"aggregate"`
}

// JrubyMetrics is a struct that holds the data for the jruby metrics
type JrubyMetrics struct {
	Version      string       `json:"service_version"`
	StateVersion int          `json:"service_status_version"`
	DetailLevel  string       `json:"detail_level"`
	State        string       `json:"state"`
	Status       *JrubyStatus `json:"status"`
}

// JrubyStatus is a struct that the experimental json which holds the correct arrays
type JrubyStatus struct {
	Experimental *JrubyExperimental `json:"experimental"`
}

// JrubyExperimental is a struct that holds the array of metrics
type JrubyExperimental struct {
	JrubyPoolLockStatus *JrubyPoolLockStatus      `json:"jruby-pool-lock-status"`
	Metrics             *JrubyExperimentalMetrics `json:"metrics"`
}

// JrubyPoolLockStatus holds metrics for jruby
type JrubyPoolLockStatus struct {
	State      string `json:"current-state"`
	ChangeTime string `json:"last-change-time"`
}

// JrubyExperimentalMetrics holds metrics for jruby
type JrubyExperimentalMetrics struct {
	AverageLockWaitTime     int                      `json:"average-lock-wait-time"`
	NumFreeJrubies          int                      `json:"num-free-jrubies"`
	BorrowCount             int                      `json:"borrow-count"`
	AverageRequestedJrubies float64                  `json:"average-requested-jrubies"`
	BorrowTimeoutCount      int                      `json:"borrow-timeout-count"`
	ReturnCount             int                      `json:"return-count"`
	BorrowRetryCount        int                      `json:"borrow-retry-count"`
	BorrowedInstances       *[]JrubyBorrowedInstance `json:"borrowed-instances"`
	AverageBorrowTime       int                      `json:"average-borrow-time"`
	NumJrubies              int                      `json:"num-jrubies"`
	RequestedCount          int                      `json:"requested-count"`
	QueueLimitHitRate       float64                  `json:"queue-limit-hit-rate"`
	AverageLockHeldTime     int                      `json:"average-lock-held-time"`
	QueueLimitHitCount      int                      `json:"queue-limit-hit-count"`
	AverageFreeJrubies      float64                  `json:"average-free-jrubies"`
	NumPoolLocks            int                      `json:"num-pool-locks"`
	AverageWaitTime         int                      `json:"average-wait-time"`
}

// JrubyBorrowedInstance holds metrics for jruby
type JrubyBorrowedInstance struct {
	Time          int                          `json:"time"`
	DurationMilis int                          `json:"duration-millis"`
	Reason        *JrubyBorrowedInstanceReason `json:"reason"`
}

// JrubyBorrowedInstanceReason holds metrics for jruby
type JrubyBorrowedInstanceReason struct {
	Request *JrubyBorrowedInstanceReasonRequest `json:"request"`
}

// JrubyBorrowedInstanceReasonRequest holds metrics for jruby
type JrubyBorrowedInstanceReasonRequest struct {
	Uri     string `json:"uri"`
	Method  string `json:"request-method"`
	RouteId string `json:"route-id"`
}

// MasterMetrics holds metrics for jruby
type MasterMetrics struct {
	Version      string        `json:"service_version"`
	StateVersion int           `json:"service_status_version"`
	DetailLevel  string        `json:"detail_level"`
	State        string        `json:"state"`
	Status       *MasterStatus `json:"status"`
}

// MasterStatus is a struct that the experimental json which holds the correct arrays
type MasterStatus struct {
	Experimental *MasterExperimental `json:"experimental"`
}

// MasterExperimental is a struct that holds the array of metrics
type MasterExperimental struct {
	HttpMetrics       *[]MasterHttpMetric       `json:"http-metrics"`
	HttpClientMetrics *[]MasterHttpClientMetric `json:"http-client-metrics"`
}

// MasterHttpMetric is a struct that holds master data
type MasterHttpMetric struct {
	RouteId   string `json:"route-id"`
	Count     int    `json:"count"`
	Mean      int    `json:"mean"`
	Aggregate int    `json:"aggregate"`
}

// MasterHttpClientMetric is a struct that holds master data
type MasterHttpClientMetric struct {
	MetricName string    `json:"metric-name"`
	MetricId   *[]string `json:"metric-id"`
	Count      int       `json:"count"`
	Mean       int       `json:"mean"`
	Aggregate  int       `json:"aggregate"`
}

// ServiceMetrics holds metrics for jruby
type ServiceMetrics struct {
	Version      string         `json:"service_version"`
	StateVersion int            `json:"service_status_version"`
	DetailLevel  string         `json:"detail_level"`
	State        string         `json:"state"`
	Status       *ServiceStatus `json:"status"`
}

// ServiceStatus is a struct that the experimental json which holds the correct arrays
type ServiceStatus struct {
	Experimental *ServiceExperimental `json:"experimental"`
}

// ServiceExperimental is a struct that holds the array of metrics
type ServiceExperimental struct {
	JVMMetrics *ServiceJVMMetric `json:"jvm-metrics"`
}

// ServiceJVMMetric is a struct that holds metrics
type ServiceJVMMetric struct {
	CpuUsage        float64                     `json:"cpu-usage"`
	UptimeMs        int                         `json:"up-time-ms"`
	GCCpuUsage      float64                     `json:"gc-cpu-usage"`
	StartTimeMs     int                         `json:"start-time-ms"`
	Threading       *ServiceJVMMetricThreading  `json:"threading"`
	HeapMemory      *ServiceJVMMetricHeapMemory `json:"heap-memory"`
	GCStats         *ServiceJVMMetricGCStats    `json:"gc-stats"`
	FileDescriptors *ServiceJVMMetricFile       `json:"file-descriptors"`
	NonHeapMemory   *ServiceJVMMetricHeapMemory `json:"non-heap-memory"`
}

// ServiceJVMMetricThreading is a struct that holds metrics
type ServiceJVMMetricThreading struct {
	ThreadCount     int `json:"thread-count"`
	PeakThreadCount int `json:"peak-thread-count"`
}

// ServiceJVMMetricHeapMemory is a struct that holds metrics
type ServiceJVMMetricHeapMemory struct {
	Committed int `json:"committed"`
	Init      int `json:"init"`
	Max       int `json:"max"`
	Used      int `json:"used"`
}

// ServiceJVMMetricGCStats is a struct that holds metrics
type ServiceJVMMetricGCStats struct {
	PSScavenge *ServiceJVMMetricPS `json:"PS Scavenge"`
	PSSweep    *ServiceJVMMetricPS `json:"PS MarkSweep"`
}

// ServiceJVMMetricPS is a struct that holds metrics
type ServiceJVMMetricPS struct {
	Count       int                       `json:"count"`
	TotalTimeMs int                       `json:"total-time-ms"`
	LastGCInfo  *ServiceJVMMetricLastInfo `json:"last-gc-info"`
}

// ServiceJVMMetricLastInfo is a struct that holds metrics
type ServiceJVMMetricLastInfo struct {
	DurationMs int `json:"duration-ms"`
}

// ServiceJVMMetricFile is a struct that holds metrics
type ServiceJVMMetricFile struct {
	Max  int `json:"max"`
	Used int `json:"used"`
}

func getURLMaster(host string, port int) string {
	return fmt.Sprintf("https://%s:%v", host, port)
}

// NewClientSSL gets a new client with ssl certs enabled
func NewClientSSLMaster(host string, port int, key string, cert string, ca string, verbose bool) *ClientMaster {
	flag.Parse()
	cert2, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		log.Println(err.Error())
	}
	// Load CA cert
	caCert, err := ioutil.ReadFile(ca)
	if err != nil {
		log.Println(err.Error())
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
	return &ClientMaster{getURLMaster(host, port), cert, key, client, verbose}

}

// NewClientSSLInsecure returns a https connection for your puppetdb instance but trusts self signed certificates.
func NewClientSSLInsecureMaster(host string, port int, verbose bool) *ClientMaster {
	flag.Parse()

	// Setup HTTPS client
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}
	tlsConfig.BuildNameToCertificate()
	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}
	return &ClientMaster{getURLMaster(host, port), "", "", client, verbose}

}

func (c *ClientMaster) httpGet(endpoint string) (resp *http.Response, err error) {
	metrics := []string{"jruby-metrics", "master", "puppet-profiler", "status-service"}
	base := strings.TrimRight(c.BaseURL, "/")
	PUrl := ""
	if stringInSlice(endpoint, metrics) {
		PUrl = fmt.Sprintf("%s/status/v1/services/%s?level=debug", base, endpoint)
	} else {
		PUrl = fmt.Sprintf("%s%s", base, endpoint)
	}
	if PUrl == "" {
		return nil, errors.New("Endpoint does not exist")
	}

	if c.verbose == true {
		log.Printf(PUrl)
	}
	return c.httpClient.Get(PUrl)
}

func (c *ClientMaster) httpPut(endpoint string, values interface{}) (resp *http.Response, err error) {
	base := strings.TrimRight(c.BaseURL, "/")
	PUrl := fmt.Sprintf("%s%s", base, endpoint)

	if c.verbose == true {
		log.Printf(PUrl)
	}
	if values != nil {
		json, err := json.Marshal(values)
		req, err := http.NewRequest(http.MethodPut, PUrl, bytes.NewBuffer(json))
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
		return c.httpClient.Do(req)

	}

	return nil, errors.New("No values specified")

}

func (c *ClientMaster) httpDelete(endpoint string) (resp *http.Response, err error) {
	base := strings.TrimRight(c.BaseURL, "/")
	PUrl := fmt.Sprintf("%s%s", base, endpoint)

	if c.verbose == true {
		log.Printf(PUrl)
	}

	req, err := http.NewRequest(http.MethodDelete, PUrl, nil)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	return c.httpClient.Do(req)
}

// Get gets the given url and retruns the result. In form of the given interface.
func (c *ClientMaster) Get(v interface{}, path string) error {

	resp, err := c.httpGet(path)
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

// Put request to the given url and returns the status code
func (c *ClientMaster) Put(v interface{}, path string, values interface{}) (error, int) {
	// https://gist.github.com/slav123/cbb3309052de5a870667
	resp, err := c.httpPut(path, values)
	statusCode := -1
	if resp != nil {
		statusCode = resp.StatusCode
	}
	if err != nil {
		log.Print(err.Error())
		return err, statusCode
	}
	defer resp.Body.Close()
	if err != nil {
		log.Print(err)
		return err, statusCode
	}
	if c.verbose {
		contents, _ := ioutil.ReadAll(resp.Body)
		log.Println(string(contents))
	}
	json.NewDecoder(resp.Body).Decode(&v)
	return err, statusCode
}

// Delete request to the given url and returns the result code
func (c *ClientMaster) Delete(path string) (error, int) {
	resp, err := c.httpDelete(path)
	statusCode := -1
	if resp != nil {
		statusCode = resp.StatusCode
	}
	if err != nil {
		log.Print(err.Error())
		return err, statusCode
	}
	defer resp.Body.Close()
	if err != nil {
		log.Print(err)
		return err, statusCode
	}
	if c.verbose {
		contents, _ := ioutil.ReadAll(resp.Body)
		log.Println(string(contents))
	}
	return err, statusCode
}

// profiler returns a profiler metrics object
func (c *ClientMaster) Profiler() (Profiler, error) {
	ret := Profiler{}
	err := c.Get(&ret, "puppet-profiler")
	return ret, err
}

// Jruby returns a jruby metrics object
func (c *ClientMaster) Jruby() (JrubyMetrics, error) {
	ret := JrubyMetrics{}
	err := c.Get(&ret, "jruby-metrics")
	return ret, err
}

// Master returns a master metrics object
func (c *ClientMaster) Master() (MasterMetrics, error) {
	ret := MasterMetrics{}
	err := c.Get(&ret, "master")
	return ret, err
}

// Master returns a master metrics object
func (c *ClientMaster) Service() (ServiceMetrics, error) {
	ret := ServiceMetrics{}
	err := c.Get(&ret, "status-service")
	return ret, err
}

// PuppetCertificatesreturns an array of puppet certificates
func (c *ClientMaster) PuppetCertificates() ([]PuppetCertificate, error) {
	ret := []PuppetCertificate{}
	err := c.Get(&ret, "/puppet-ca/v1/certificate_statuses/any")
	return ret, err
}

// PuppetCertificate returns a single entry of a puppet certificate
func (c *ClientMaster) PuppetCertificate(certname string) (PuppetCertificate, error) {
	ret := PuppetCertificate{}
	// /puppet-ca/v1/certificate/
	err := c.Get(&ret, "/puppet-ca/v1/certificate_status/"+certname)
	return ret, err
}

// PuppetCertificateUpdateStatereturns a single entry of a puppet certificate
func (c *ClientMaster) PuppetCertificateUpdateState(certname string, state string) (PuppetCertificateState, error, int) {
	ret := PuppetCertificateState{}
	st := PuppetCertificateState{DesiredState: state}
	// /puppet-ca/v1/certificate/
	err, code := c.Put(&ret, "/puppet-ca/v1/certificate_status/"+certname, st)
	return ret, err, code
}

// PuppetCertificateDelete deletes a certificate entry
func (c *ClientMaster) PuppetCertificateDelete(certname string) (error, int) {
	// /puppet-ca/v1/certificate/
	err, code := c.Delete("/puppet-ca/v1/certificate_status/" + certname)
	return err, code
}

// stringInSlice checks wether a string is in a slice https://stackoverflow.com/questions/15323767/does-go-have-if-x-in-construct-similar-to-python
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
