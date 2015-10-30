## onyx-elasticsearch

Onyx plugin for for ElasticSearch 1.x.  Currently only provides write operations, but will provide read/query operations in the future.  For more details on ElasticSearch please read the [official documentation](https://www.elastic.co/guide/index.html). 

#### Installation

In your project file:

```clojure
[onyx-elasticsearch "0.7.14"]
```

In your peer boot-up namespace:

```clojure
(:require [onyx.plugin.elasticsearch])
```

#### Functions

##### write-messages

Catalog entry:

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

Lifecycle entry:

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

###### Attributes

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

#### Acknowledgements

This plugin leverages the [clojurewerkz/elastisch](https://github.com/clojurewerkz/elastisch) library for all ElasticSearch communication.

#### Contributing

Pull requests into the master branch are welcomed.

#### License

Copyright © 2015 Matt Anderson

Distributed under the Eclipse Public License, the same as Clojure.
