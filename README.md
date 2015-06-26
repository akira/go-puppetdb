# go-puppetdb

An API client interface for [PuppetDB](https://docs.puppetlabs.com/puppetdb/latest/)

Please note that there since I wrote this, there is now a PuppetDB client implementation by Ken Barber from Puppet that you can also choose to use: [https://github.com/kbarber/puppetdb-client-go](https://github.com/kbarber/puppetdb-client-go)

## Background

Package contains interface to PuppetDB v3 API.  Interface is still work in progress and does not cover the entire API. 

## Installation

Run `go get github.com/akira/go-puppetdb`.

## Usage


```go
import (
  "github.com/akira/go-puppetdb"
)
```

Create a Client with PuppetDB Hostname:

```go
// second parameter enables verbose output
client := puppetdb.NewClient("http://127.0.0.1:8080", true)

resp, err := client.Nodes()
...
resp, err := client.NodeFacts("node123")
...
```


Queries can be represented as an array of strings and turned into JSON:

```go
query, err := puppetdb.QueryToJson([]string{"=", "report", "aef00"})
resp, res_err := client.Events(query, nil)
```

# Contributors

Malte Krupa (temal-)

Will Roberts (willroberts)

