(ns onyx.plugin.elasticsearch
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [clojurewerkz.elastisch.native  :as es]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.native.document]
            [clojurewerkz.elastisch.rest.document]
            [taoensso.timbre :as log]))

(defn- write-as
  [type op & args]
  (let [nsp (if (= type :native) "clojurewerkz.elastisch.native.document/" "clojurewerkz.elastisch.rest.document/")]
    (println "Sending:" (resolve (symbol (str nsp (name op)))) (flatten args))
    (apply (resolve (symbol (str nsp (name op)))) (flatten args))))

(defn- write-elasticsearch [cxn doc settings]
  (let [client-type (:elasticsearch/client-type settings)
        index (:elasticsearch/index settings)
        mapping (:elasticsearch/mapping settings)
        doc-id (:elasticsearch/doc-id settings)]
    (case (:elasticsearch/write-type settings)
      :insert (write-as client-type :create cxn index mapping doc :id doc-id)
      :upsert (write-as client-type :put cxn index mapping doc-id doc)
      :delete (write-as client-type :delete cxn index mapping doc-id))))

(defn inject-writer
  [{{host :elasticsearch/host
     port :elasticsearch/port
     cluster-name :elasticsearch/cluster-name
     http-ops :elasticsearch/http-ops
     client-type :elasticsearch/client-type
     index :elasticsearch/index
     doc-id :elasticsearch/doc-id
     mapping :elasticsearch/mapping
     write-type :elasticsearch/write-type
     :or {http-ops {}
          client-type :http
          mapping "_default_"
          write-type :insert}} :onyx.core/task-map} _]
  {:pre [(not (empty? host))
         (and (number? port) (< 0 port 65536))
         (some #{client-type} [:http :native])
         (or (= client-type :http) (not (empty? cluster-name)))
         (some #{write-type} [:insert :upsert :delete])
         (or (not= write-type :delete) (not (empty? doc-id)))]}
  (log/info (str "Created ElasticSearch " client-type " client for " host ":" port))
  {:elasticsearch/connection (if
                               (= client-type :http)
                               (esr/connect (str "http://" host ":" port) http-ops)
                               (es/connect [[host port]] {"cluster.name" cluster-name}))
   :elasticsearch/doc-defaults {:elasticsearch/index index
                                :elasticsearch/doc-id doc-id
                                :elasticsearch/mapping mapping
                                :elasticsearch/write-type write-type
                                :elasticsearch/client-type client-type}})

(def write-messages-calls
  {:lifecycle/before-task-start inject-writer})

(defrecord ElasticsearchWrite []
  p-ext/Pipeline
  (read-batch
    [_ event]
    (function/read-batch event))

  (write-batch
    [_ {results :onyx.core/results
        connection :elasticsearch/connection
        defaults :elasticsearch/doc-defaults}]
    (doseq [msg (mapcat :leaves (:tree results))]
      (let [document (or (:elasticsearch/message (:message msg)) (:message msg))
            settings (if
                       (contains? (:message msg) :elasticsearch/message)
                       (merge defaults (select-keys
                                         (:message msg) [:elasticsearch/index
                                                         :elasticsearch/doc-id
                                                         :elasticsearch/mapping
                                                         :elasticsearch/write-type]))
                       defaults)]
        (log/debug (str "Message Settings: " settings))
        (write-elasticsearch connection document settings)))
    {})

  (seal-resource
    [_ _]
    {}))

(defn write-messages [_]
  (->ElasticsearchWrite))