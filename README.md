## onyx-elasticsearch

[Onyx](https://github.com/onyx-platform/onyx) plugin providing write for batch processing an ElasticSearch 5.x cluster.  For more details on ElasticSearch please read the [official documentation](https://www.elastic.co/guide/index.html).

**Note on the Version Number Format**:  The first three numbers in the version correspond to the latest Onyx platform release.  The final digit increments when there are changes to the plugin within an Onyx release.

### Installation

In your project file:

```clojure
[org.onyxplatform/onyx-elasticsearch "0.12.2.1-alpha1"]
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
 :elasticsearch/sort {:foo "desc"}
 :elasticsearch/restart-on-fail false
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

| key                            | type      | default     | description
|--------------------------------|-----------|-------------|-------------
|`:elasticsearch/host`           | `string`  |             | ElasticSearch Host.  Required.
|`:elasticsearch/port`           | `number`  |             | ElasticSearch Port.  Required.
|`:elasticsearch/protocol`       | `keyword` | `:http`     | Protocol to use when connecting to ElasticSearch. Should be either `:http` or `:https`. Only applies when using `:client-type` of `:http`.
|`:elasticsearch/cluster-name`   | `string`  |             | ElasticSearch Cluster Name.  Required for native connections.
|`:elasticsearch/client-type`    | `keyword` | `:http`     | Type of client to create.  Should be either `:http` or `:native`.
|`:elasticsearch/http-ops`       | `map`     |             | Additional, optional HTTP Options used by the HTTP Client for connections.  Includes any options allowed by the [clj-http library](https://github.com/dakrone/clj-http#usage).
|`:elasticsearch/index`          | `string`  |             | The index to search for the document in.  If not provided, all indexes will be searched.
|`:elasticsearch/mapping`        | `string`  |             | The name of the ElasticSearch mapping to search for documents in.  If not provided, all mappings will be searched.
|`:elasticsearch/query`          | `map`     |             | A Clojure map with the same structure as an [ElasticSearch JSON Query](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html).  If not provided, all documents will be returned.  
|`:elasticsearch/sort`           | `map`     | `"_score"`  | A Clojure map with the same structure as an [ElasticSearch JSON Sort](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html).
|`:elasticsearch/restart-on-fail`| `boolean` | `false`     | If `true` the entire query will be run again from scratch in the event of a peer failure.  Otherwise, will re-run from last offset.  See below section: "Message Guarantees"


**Response Segment**:

If the query does not match any documents, then the `:done` sentinel will be returned signaling that there is no further processing for that query.  Otherwise, the response segment will be a clojure map with data and meta-data from ElasticSearch.  An example segment resulting from a search for a document `{:foo "bar"}`:

```clojure
{:_index  "idx-id"
 :_type   "mapping-type"
 :_id     "doc-id"
 :_score  0.30685282
 :_source {:foo "bar"}}
```

**Message Guarantees & Fault Tolerance**

In general, Onyx offers an "at-least-once" message processing guarantee.  This same guarantee should extend to input plugins as well including situations where there is a peer failure processing the input from source.  However, because of the nature of ElasticSearch, there are situations where this guarantee is not possible with this plugin if a peer fails and there are updates to messages involved in the query concurrent to the processing job.  If there are no concurrent modifications (or a peer does not fail), then the "at-least-once" guarantee stands.

There is also variation on how this plugin supports a "[happened-before](https://en.wikipedia.org/wiki/Happened-before)" relationship when updates to documents are being made concurrent to workflow execution and a failure occurs.  Without failures, the plugin will only process documents that were created/modified prior to the execution of the query regardless of concurrent activity.  However, in a failure, the query must be re-run and so there is variation on this guarantee.

There are two options available to handle fault tolerance using the `:elasticsearch/restart-on-fail` catalog parameter as well as some additional manual steps that can be taken.  This section is intended to help the workflow designer decide how to best configure fault tolerance to meet the message processing requirements of their application.

`:elasticsearch/restart-on-fail = TRUE`

When `true` the entire query will be restarted from scratch in the event of a failure.  This will guarantee that all messages will be processed at least once, however depending on how many messages processed before the peer failure, there could be a large number of duplicate messages processed from the query.  For concurrent creates and updates all messages processed prior to the last restarted query will be seen.  The exception is concurrent deletes, which can cause non-deterministic results if a message is deleted after the original processing, but before a restart query.
 
This setting is recommended when the query does not return a prohibitively large number of results and every message needs to be processed (assuming, of course, that processing is idempotent, which should be the case anyway in an "at-least-once" system).

Additionally, the user can leverage the [Onyx State Management](https://onyx-platform.gitbooks.io/onyx/content/doc/user-guide/aggregation-state-management.html) functionality to only process messages once.  This can improve the message processing guarantee to "exactly-once" and a "happened-before" the original query message relationship (the exception being deletes).

`:elasticsearch/restart-on-fail = FALSE`

When `false` the query will be restarted from the last acked offset.  This can provide significant savings when the query is large since, in the event of a failure, the already processed messages do not get re-triggered.  If there are no concurrent modifications to documents in the query or only creates and updates are being processed, then this will guarantee at least once message processing.  When there are concurrent deletes, the results are non-deterministic since messages removed before the current offset can affect the order on re-queries.  In all cases of concurrent modification, happened-before is non-deterministic with this setting.

This setting is recommended when the query is large and/or there are either no concurrent modifications or precise processing isn't necessary (E.G.: calculating statistics on a large data set may have tolerance for a couple of lost messages in the event of a failure).

Additionally, the user can add an ascending [ElasticSearch sort criteria](https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-sort.html) on a field that increments per message such as a timestamp or incrementing counter so that any new messages created will be returned last in a query response providing a "happened-before" the original query relationship when the only concurrent operation is creates.


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
 :elasticsearch/index "my-index-name"
 :elasticsearch/mapping-type "my-mapping-name"
 :elasticsearch/id "my-id"
 :elasticsearch/write-type :index
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
  * `:elasticsearch/mapping-type`
  * `:elasticsearch/id`
  * `:elasticsearch/write-type`
* If the map does NOT contain an `:elasticsearch/message` key, then the entire input segment will be treated as the document for ElasticSearch and the default settings from the catalog will be used.

**Attributes**

| key                              | type      | default     | description
|----------------------------------|-----------|-------------|-------------
|`:elasticsearch/host`             | `string`  |             | ElasticSearch Host.  Required.
|`:elasticsearch/port`             | `number`  |             | ElasticSearch Port.  Required.
|`:elasticsearch/http-ops`         | `map`     |             | Additional, optional HTTP Options used by the HTTP Client for connections.  Includes any options allowed by the [clj-http library](https://github.com/dakrone/clj-http#usage).
|`:elasticsearch/index`            | `string`  |             | The index to store the document in.  Required in either Catalog or Segment.
|`:elasticsearch/mapping-type`     | `string`  |`"_default_"`| The name of the ElasticSearch mapping to use.
|`:elasticsearch/id`               | `string`  |             | Unique id of the document.  Required only for delete, otherwise ElasticSearch will generate a unique id if not supplied for insert/upsert operations.
|`:elasticsearch/write-type`       | `keyword` |`:index`     | Type of write to perform.  Should be one of `:index`, `update`, `update-by-query`, `:upsert`, `:delete`.

### Acknowledgements

This plugin leverages the [cc.qbits/spandex](https://github.com/mpenet/spandex) library for all Elasticsearch communication.

This library was adapted from an original library written by Matt Anderson targetting the [clojurewerkz/elastisch] library.

### Contributing

Pull requests into the master branch are welcomed.

### License

Copyright © 2015 Matt Anderson
Copyright © 2018 David Bernal

Distributed under the Eclipse Public License, the same as Clojure.
