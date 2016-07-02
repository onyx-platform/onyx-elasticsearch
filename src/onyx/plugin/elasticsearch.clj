(ns onyx.plugin.elasticsearch
  (:require [onyx.peer.function :as function]
            [onyx.extensions :as extensions]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [clojure.core.async :refer [chan go timeout <!! >!! alts!! sliding-buffer go-loop close! poll!]]
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
  [client-type conn index mapping query sort start-index scroll]
  (let [query-list (if query [:query query] [])
        sort-list (if sort [:sort sort] [])]
    (->
      (cond
        (and index mapping) (run-as client-type :search conn index mapping query-list sort-list :from start-index :scroll scroll)
        (not (nil? index)) (run-as client-type :search-all-types conn index query-list sort-list :from start-index :scroll scroll)
        :else (run-as client-type :search-all-indexes-and-types conn query-list sort-list :from start-index :scroll scroll)))))

(defn- start-commit-loop! [write-chunk? commit-ch log k]
  (go-loop []
    (when-let [content (<!! commit-ch)]
      (when write-chunk?
        (extensions/force-write-chunk log :chunk content k))
      (recur))))

(defn inject-reader
  [{{max-peers :onyx/max-peers
     host :elasticsearch/host
     port :elasticsearch/port
     cluster-name :elasticsearch/cluster-name
     http-ops :elasticsearch/http-ops
     client-type :elasticsearch/client-type
     index :elasticsearch/index
     mapping :elasticsearch/mapping
     query :elasticsearch/query
     sort :elasticsearch/sort
     restart-on-fail :elasticsearch/restart-on-fail
     :or {http-ops {}
          client-type :http
          mapping "_default_"
          sort "_doc"}} :onyx.core/task-map
    {read-ch :read-ch
     retry-ch :retry-ch
     commit-ch :commit-ch} :onyx.core/pipeline
    log :onyx.core/log
    job-id :onyx.core/job-id
    task-id :onyx.core/task-id} _]
  {:pre [(= 1 max-peers)
         (not (empty? host))
         (and (number? port) (< 0 port 65536))
         (some #{client-type} [:http :native])
         (or (= client-type :http) (not (empty? cluster-name)))
         (or (or (= sort "_doc") (= sort "_score")) (not= mapping "_default_"))]}
  (let [job-task-id  (str job-id "#" task-id)
        _ (extensions/write-chunk log :chunk {:chunk-index -1 :status :incomplete} job-task-id)
        content (extensions/read-chunk log :chunk job-task-id)]
    (if (= :complete (:status content))
      (do
        (log/warn (str "Restarted task " task-id " that was already complete.  No action will be taken."))
        (>!! read-ch (t/input (java.util.UUID/randomUUID) :done))
        {})
      (do
        (log/info (str "Creating ElasticSearch " client-type " client for " host ":" port))
        (let [_ (start-commit-loop! (not restart-on-fail) commit-ch log job-task-id)
              conn (create-es-client client-type host port cluster-name http-ops)
              start-index (:chunk-index content)
              scroll-time "1m"
              res (query-es client-type conn index mapping query sort (inc start-index) scroll-time)]
          (loop [rs (run-as client-type :scroll-seq conn res)
                 chunk-idx (inc start-index)]
            (when-let [msg (first rs)]
              (>!! read-ch (assoc (t/input (java.util.UUID/randomUUID) msg) :chunk-index chunk-idx))
              (recur (next rs) (inc chunk-idx))))
          (>!! read-ch (t/input (java.util.UUID/randomUUID) :done))
          {:elasticsearch/connection conn
           :elasticsearch/read-ch read-ch
           :elasticsearch/retry-ch retry-ch
           :elasticsearch/commit-ch commit-ch
           :elasticsearch/doc-defaults {:elasticsearch/index index
                                        :elasticsearch/mapping mapping
                                        :elasticsearch/query query
                                        :elasticsearch/client-type client-type}})))))

(defn close-read-resources
  [{:keys [elasticsearch/producer-ch elasticsearch/commit-ch elasticsearch/read-ch elasticsearch/retry-ch] :as event} lifecycle]
  (close! read-ch)
  (close! retry-ch)
  (while (poll! read-ch))
  (while (poll! retry-ch))
  (close! commit-ch)
  {})

(def read-messages-calls
  {:lifecycle/before-task-start inject-reader
   :lifecycle/after-task-stop close-read-resources})

(defn- highest-acked-chunk [starting-index max-index pending-chunk-indices]
  (loop [max-pending starting-index]
    (if (or (pending-chunk-indices (inc max-pending))
            (= max-index max-pending))
      max-pending
      (recur (inc max-pending)))))

(defn- all-done? [messages]
  (empty? (remove #(= :done (:message %))
                  messages)))

(defrecord ElasticsearchRead [max-pending batch-size batch-timeout pending-messages drained?
                              top-chunk-index top-acked-chunk-index pending-chunk-indices
                              read-ch retry-ch commit-ch]
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
                       (keep (fn [_]
                               (let [[result ch] (alts!! [retry-ch read-ch timeout-ch] :priority true)]
                                 result)))))]
      (doseq [m batch]
        (when-let [chunk-index (:chunk-index m)]
          (swap! top-chunk-index max chunk-index)
          (swap! pending-chunk-indices conj chunk-index))
        (swap! pending-messages assoc (:id m) m))
      (when (and (all-done? (vals @pending-messages))
                 (all-done? batch)
                 (zero? (count (.buf read-ch)))
                 (zero? (count (.buf retry-ch)))
                 (or (not (empty? @pending-messages))
                     (not (empty? batch))))
        (>!! commit-ch {:status :complete})
        (reset! drained? true))
      {:onyx.core/batch batch}))

  (seal-resource [_ _])

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    (let [chunk-index (:chunk-index (@pending-messages segment-id))]
      (swap! pending-chunk-indices disj chunk-index)
      (let [new-top-acked (highest-acked-chunk @top-acked-chunk-index @top-chunk-index @pending-chunk-indices)]
        (>!! commit-ch {:chunk-index new-top-acked :status :incomplete})
        (reset! top-acked-chunk-index new-top-acked))
      (swap! pending-messages dissoc segment-id)))

  (retry-segment
    [_ _ segment-id]
    (when-let [msg (get @pending-messages segment-id)]
      (swap! pending-messages dissoc segment-id)
      (>!! retry-ch (t/input (java.util.UUID/randomUUID)
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
        read-ch (chan 1000)
        retry-ch (chan (* 2 max-pending))
        commit-ch (chan (sliding-buffer 1))]
    (->ElasticsearchRead max-pending batch-size batch-timeout pending-messages drained?
                         (atom -1) (atom -1) (atom #{}) read-ch retry-ch commit-ch)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Writer
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn- write-elasticsearch [cxn doc settings]
  (let [client-type (:elasticsearch/client-type settings)
        index (:elasticsearch/index settings)
        mapping (:elasticsearch/mapping settings)
        doc-id (:elasticsearch/doc-id settings)
        write-type (if doc-id
                     (:elasticsearch/write-type settings)
                     (keyword (str (name (:elasticsearch/write-type settings)) "-noid")))]
    (case write-type
      :insert (run-as client-type :create cxn index mapping doc :id doc-id)
      :insert-noid (run-as client-type :create cxn index mapping doc)
      :upsert (run-as client-type :put cxn index mapping doc-id doc)
      :upsert-noid (run-as client-type :create cxn index mapping doc)
      :delete (run-as client-type :delete cxn index mapping doc-id)
      :default (throw (Exception. (str "Invalid write type: " write-type))))))

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
