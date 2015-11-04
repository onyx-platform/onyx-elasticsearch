## onyx-elasticsearch

[Onyx](https://github.com/onyx-platform/onyx) plugin providing query and write for batch processing an ElasticSearch 1.x cluster.  For more details on ElasticSearch please read the [official documentation](https://www.elastic.co/guide/index.html).

**Note on the Version Number Format**:  The first three numbers in the version correspond to the latest Onyx platform release.  The final digit increments when there are changes to the plugin within an Onyx release.

### Installation

In your project file:

```clojure
[onyx-elasticsearch "0.7.14.0"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.elasticsearch])
```

### Functions

#### read-messages

Reads documents from an ElasticSearch cluster with a specified query and submits them to the Onyx workflow for processing.

**Catalog entry**:

```clojure
{:onyx/name :read-messages
 :onyx/plugin :onyx.plugin.elasticsearch/read-messages
 :onyx/type :input
 :onyx/medium :elasticsearch
 :elasticsearch/host "127.0.0.1"
 :elasticsearch/port 9200
 :elasticsearch/cluster-name "my-cluster-name"
 :elasticsearch/client-type :http
 :elasticsearch/http-ops {:basic-auth ["user" "pass"]}
 :elasticsearch/index "my-index-name"
 :elasticsearch/mapping "my-mapping-name"
 :elasticsearch/query {:term {:foo "bar"}}
 :onyx/batch-size batch-size
 :onyx/max-peers 1
 :onyx/doc "Read documents from an ElasticSearch Query"}
```

**Lifecycle entry**:

```clojure
{:lifecycle/task :read-messages
    :lifecycle/calls :onyx.plugin.elasticsearch/read-messages-calls}
```

**Attributes**

| key                         | type      | default     | description
|-----------------------------|-----------|-------------|-------------
|`:elasticsearch/host`        | `string`  |             | ElasticSearch Host.  Required.
|`:elasticsearch/port`        | `number`  |             | ElasticSearch Port.  Required.
|`:elasticsearch/cluster-name`| `string`  |             | ElasticSearch Cluster Name.  Required for native connections.
|`:elasticsearch/client-type` | `keyword` |`:http`      | Type of client to create.  Should be either `:http` or `:native`.
|`:elasticsearch/http-ops`    | `map`     |             | Additional, optional HTTP Options used by the HTTP Client for connections.  Includes any options allowed by the [clj-http library](https://github.com/dakrone/clj-http#usage).
|`:elasticsearch/index`       | `string`  |             | The index to search for the document in.  If not provided, all indexes will be searched.
|`:elasticsearch/mapping`     | `string`  |             | The name of the ElasticSearch mapping to search for documents in.  If not provided, all mappings will be searched.
|`:elasticsearch/query`       | `map`     |             | A Clojure map with the same structure as an [ElasticSearch JSON Query](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html).  If not provided, all documents will be returned.  

#### write-messages

Creates, Updates, or Deletes documents from an ElasticSearch cluster.

**Catalog entry**:

```clojure
{:onyx/name :write-messages
 :onyx/plugin :onyx.plugin.elasticsearch/write-messages
 :onyx/type :output
 :onyx/medium :elasticsearch
 :elasticsearch/host "127.0.0.1"
 :elasticsearch/port 9200
 :elasticsearch/cluster-name "my-cluster-name"
 :elasticsearch/client-type :http
 :elasticsearch/http-ops {:basic-auth ["user" "pass"]}
 :elasticsearch/index "my-index-name"
 :elasticsearch/mapping "my-mapping-name"
 :elasticsearch/doc-id "my-id"
 :elasticsearch/write-type :insert
 :onyx/batch-size batch-size
 :onyx/doc "Writes documents to elasticsearch"}
```

**Lifecycle entry**:

```clojure
[{:lifecycle/task :write-messages
  :lifecycle/calls :onyx.plugin.elasticsearch/write-messages-calls}]
```

Segments supplied to a write-messages task should be a Clojure map and can take one of two forms:
* Map containing the message in the following form: `{:elasticsearch/message message-body}` where `message-body` is a Clojure map representing the document to send to ElasticSearch.  The map can also contain the following optional attributes (defined in detail in the table below), which overwrite those specified in the catalog:
  * `:elasticsearch/index`
  * `:elasticsearch/mapping`
  * `:elasticsearch/doc-id`
  * `:elasticsearch/write-type`
* If the map does NOT contain an `:elasticsearch/message` key, then the entire input segment will be treated as the document for ElasticSearch and the default settings from the catalog will be used.

**Attributes**

| key                         | type      | default     | description
|-----------------------------|-----------|-------------|-------------
|`:elasticsearch/host`        | `string`  |             | ElasticSearch Host.  Required.
|`:elasticsearch/port`        | `number`  |             | ElasticSearch Port.  Required.
|`:elasticsearch/cluster-name`| `string`  |             | ElasticSearch Cluster Name.  Required for native connections.
|`:elasticsearch/client-type` | `keyword` |`:http`      | Type of client to create.  Should be either `:http` or `:native`.
|`:elasticsearch/http-ops`    | `map`     |             | Additional, optional HTTP Options used by the HTTP Client for connections.  Includes any options allowed by the [clj-http library](https://github.com/dakrone/clj-http#usage).
|`:elasticsearch/index`       | `string`  |             | The index to store the document in.  Required in either Catalog or Segment.
|`:elasticsearch/mapping`     | `string`  |`"_default_"`| The name of the ElasticSearch mapping to use.
|`:elasticsearch/doc-id`      | `string`  |             | Unique id of the document.  Required only for delete, otherwise ElasticSearch will generate a unique id if not supplied for insert/upsert operations.
|`:elasticsearch/write-type`  | `keyword` |`:insert`    | Type of write to perform.  Should be one of `:insert`, `:upsert`, `:delete`.  The difference between `:insert` and `:upsert` is that `:insert` will fail if document already exists, while `:upsert` will update the version in ElasticSearch with the submitted document.

### Acknowledgements

This plugin leverages the [clojurewerkz/elastisch](https://github.com/clojurewerkz/elastisch) library for all ElasticSearch communication.

### Contributing

Pull requests into the master branch are welcomed.

### License

Copyright © 2015 Matt Anderson

Distributed under the Eclipse Public License, the same as Clojure.
