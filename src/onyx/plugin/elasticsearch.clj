(ns onyx.plugin.elasticsearch
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [clojure.core.async :refer [chan go timeout <!! >!! alts!!]]
            [clojurewerkz.elastisch.native  :as es]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.native.document]
            [clojurewerkz.elastisch.rest.document]
            [taoensso.timbre :as log]))

(defn- contains-some?
  [col & keys]
  (some true? (map #(contains? col %) keys)))

(defn- create-es-client
  [client-type host port cluster-name http-ops]
  (if
    (= client-type :http)
    (esr/connect (str "http://" host ":" port) http-ops)
    (es/connect [[host port]] {"cluster.name" cluster-name})))

(defn- run-as
  [type op & args]
  (let [nsp (if (= type :native) "clojurewerkz.elastisch.native.document/" "clojurewerkz.elastisch.rest.document/")]
    (apply (resolve (symbol (str nsp (name op)))) (flatten args))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Reader
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- query-es
  [client-type conn index mapping query]
  (let [query-list (if query [:query query] [])]
    (->
      (cond
        (and index mapping) (run-as client-type :search conn index mapping query-list)
        (not (nil? index)) (run-as client-type :search-all-types conn index query-list)
        :else (run-as client-type :search-all-indexes-and-types conn query-list))
      (get-in [:hits :hits]))))

(defn inject-reader
  [{{host :elasticsearch/host
     port :elasticsearch/port
     cluster-name :elasticsearch/cluster-name
     http-ops :elasticsearch/http-ops
     client-type :elasticsearch/client-type
     index :elasticsearch/index
     mapping :elasticsearch/mapping
     query :elasticsearch/query
     :or {http-ops {}
          client-type :http}} :onyx.core/task-map
    {ch :read-ch} :onyx.core/pipeline} _]
  {:pre [(not (empty? host))
         (and (number? port) (< 0 port 65536))
         (some #{client-type} [:http :native])
         (or (= client-type :http) (not (empty? cluster-name)))]}
  (log/info (str "Creating ElasticSearch " client-type " client for " host ":" port))
  (let [conn (create-es-client client-type host port cluster-name http-ops)
        res (query-es client-type conn index mapping query)]
    (if (empty? res)
      (>!! ch (t/input (java.util.UUID/randomUUID) :done))
      (doseq [msg (conj res :done)]
        (>!! ch (t/input (java.util.UUID/randomUUID) msg))))
    {:elasticsearch/connection conn
     :elasticsearch/read-ch ch
     :elasticsearch/doc-defaults {:elasticsearch/index index
                                  :elasticsearch/mapping mapping
                                  :elasticsearch/query query
                                  :elasticsearch/client-type client-type}}))

(def read-messages-calls
  {:lifecycle/before-task-start inject-reader})

(defn input-drained? [pending-messages batch]
  (and (= 1 (count @pending-messages))
       (= (count batch) 1)
       (= (:message (first batch)) :done)))

(defrecord ElasticsearchRead [max-pending batch-size batch-timeout pending-messages drained? read-ch]
  p-ext/Pipeline
  (write-batch
    [_ event]
    (function/write-batch event))

  (read-batch [_ _]
    (let [pending (count (keys @pending-messages))
          max-segments (min (- max-pending pending) batch-size)
          timeout-ch (timeout batch-timeout)
          batch (if (zero? max-segments)
                  (<!! timeout-ch)
                  (->> (range max-segments)
                       (map (fn [_]
                              (let [result (first (alts!! [read-ch timeout-ch] :priority true))]
                                (if (= (:message result) :done)
                                  (t/input (java.util.UUID/randomUUID) :done)
                                  result))))
                       (filter :message)))]
      (doseq [m batch]
        (swap! pending-messages assoc (:id m) m))
      (when (input-drained? pending-messages batch)
        (reset! drained? true))
      {:onyx.core/batch batch}))

  (seal-resource [_ _])

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    (swap! pending-messages dissoc segment-id))

  (retry-segment
    [_ _ segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id)
      (>!! read-ch (t/input (java.util.UUID/randomUUID)
                            (:message msg)))))

  (pending?
    [_ _ segment-id]
    (get @pending-messages segment-id))

  (drained?
    [_ _]
    @drained?))

(defn read-messages
  [event]
  (let [task-map (:onyx.core/task-map event)
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        pending-messages (atom {})
        drained? (atom false)
        ch (chan 1000)]
    (->ElasticsearchRead max-pending batch-size batch-timeout pending-messages drained? ch)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- write-elasticsearch [cxn doc settings]
  (let [client-type (:elasticsearch/client-type settings)
        index (:elasticsearch/index settings)
        mapping (:elasticsearch/mapping settings)
        doc-id (:elasticsearch/doc-id settings)]
    (case (:elasticsearch/write-type settings)
      :insert (run-as client-type :create cxn index mapping doc :id doc-id)
      :upsert (run-as client-type :put cxn index mapping doc-id doc)
      :delete (run-as client-type :delete cxn index mapping doc-id))))

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
  (log/info (str "Creating ElasticSearch " client-type " client for " host ":" port))
  {:elasticsearch/connection (create-es-client client-type host port cluster-name http-ops)
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
                       (or (= :delete (:elasticsearch/write-type defaults))
                           (contains-some? (:message msg) :elasticsearch/message :elasticsearch/write-type))
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