(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [onyx.util.helper :as u]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async]
            [onyx.api]
            [onyx.plugin.elasticsearch]
            [clojurewerkz.elastisch.rest.document :as esrd]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.rest.response :as esrsp]))

;; ElasticSearch should be running locally on
;; standard ports (http: 9200, native: 9300)
;; prior to running the tests

(def id (str (java.util.UUID/randomUUID)))

(def zk-addr "127.0.0.1:2188")

(def env-config 
  {:onyx/id id
   :zookeeper/address zk-addr
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config 
  {:onyx/id id
   :zookeeper/address zk-addr
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port-range [40200 40260]
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 1)

(def batch-size 20)

(def workflow [[:in :write-messages]])

;; TODO: Catalog scenarios
(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :write-messages
    :onyx/plugin :onyx.plugin.elasticsearch/write-messages
    :onyx/type :output
    :onyx/medium :elasticsearch
    :elasticsearch/host "127.0.0.1"
    :elasticsearch/port 9200
    :elasticsearch/cluster-name (u/es-cluster-name "127.0.0.1" 9200)
    :elasticsearch/client-type :http
    :elasticsearch/http-ops {}
    :elasticsearch/index id
    :elasticsearch/mapping "_default_"
    :elasticsearch/write-type :insert
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Documentation for your datasink"}])

(def in-chan (chan (inc n-messages)))

(defn inject-in-ch [_ _]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :write-messages
    :lifecycle/calls :onyx.plugin.elasticsearch/write-messages-calls}])

;; TODO: Inject input data
(doseq [n (range n-messages)]
  (>!! in-chan {:elasticsearch/message {:n n} :elasticsearch/doc-id n}))

(>!! in-chan :done)
(close! in-chan)

(def v-peers (onyx.api/start-peers 2 peer-group))

(def job-info 
  (onyx.api/submit-job
    peer-config
    {:catalog catalog
     :workflow workflow
     :lifecycles lifecycles
     :task-scheduler :onyx.task-scheduler/balanced}))

(info "Awaiting job completion")

(onyx.api/await-job-completion peer-config (:job-id job-info))

;; TODO: Testing!!
(let [conn (u/connect-rest-client)]

  (deftest check-insert
    (testing "Segments successfully written to ElasticSearch via :insert"
      (doseq [n (range n-messages)]
        (let [res (esrd/search conn id "_default_" :query (q/term :n n))]
          (is (= 1 (esrsp/total-hits res))))))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
