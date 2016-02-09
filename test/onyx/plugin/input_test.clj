(ns onyx.plugin.input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [taoensso.timbre :refer [info]]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.elasticsearch]
            [onyx.api]
            [onyx.util.helper :as u]
            [clojurewerkz.elastisch.rest.document :as esrd]))

;; ElasticSearch should be running locally on standard ports
;; (http: 9200, native: 9300) prior to running these tests.

(def id (java.util.UUID/randomUUID))

(def zk-addr "127.0.0.1:2188")

(def es-host "127.0.0.1")

(def es-rest-port 9200)

(def es-native-port 9300)

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
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 10)

(def batch-size 20)

(def workflow [[:read-messages :out]])

;; Create catalogs with different search scenarios

(def catalog-base
  [{:onyx/name :read-messages
    :onyx/plugin :onyx.plugin.elasticsearch/read-messages
    :onyx/type :input
    :onyx/medium :elasticsearch
    :elasticsearch/host es-host
    :elasticsearch/cluster-name (u/es-cluster-name)
    :elasticsearch/http-ops {}
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Read messages from an ElasticSearch Query"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def catalog-http-q&map&idx
  (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
                                     :elasticsearch/client-type :http
                                     :elasticsearch/index (.toString id)
                                     :elasticsearch/mapping "_default_"
                                     :elasticsearch/query {:term {:foo "bar"}}}))

(def catalog-http-q&idx
  (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
                                     :elasticsearch/client-type :http
                                     :elasticsearch/index (.toString id)
                                     :elasticsearch/query {:term {:foo "bar"}}}))

(def catalog-http-q&all
  (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
                                     :elasticsearch/client-type :http
                                     :elasticsearch/query {:term {:foo "bar"}}}))

(def catalog-http-idx
  (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
                                     :elasticsearch/index (.toString id)
                                     :elasticsearch/client-type :http}))

(def catalog-native-q&map&idx
  (update-in catalog-http-q&map&idx [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

(def catalog-native-q&idx
  (update-in catalog-http-q&idx [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

(def catalog-native-q&all
  (update-in catalog-http-q&all [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

(def catalog-native-idx
  (update-in catalog-http-idx [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def batch-num (atom 0))

(def read-crash
  {:lifecycle/before-batch
   (fn [event lifecycle]
     ; give the peer a bit of time to write the chunks out and ack the batches,
     ; since we want to ensure that the batches aren't re-read on restart for ease of testing
     (Thread/sleep 7000)
     (when (= (swap! batch-num inc) 2)
       (throw (ex-info "Restartable" {:restartable? true}))))
   :lifecycle/handle-exception (constantly true)})

(def lifecycles
  [{:lifecycle/task :read-messages
    :lifecycle/calls :onyx.plugin.elasticsearch/read-messages-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def lifecycles-fail
  [{:lifecycle/task :read-messages
    :lifecycle/calls :onyx.plugin.elasticsearch/read-messages-calls}
   {:lifecycle/task :read-messages
    :lifecycle/calls ::read-crash}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(def v-peers (onyx.api/start-peers 2 peer-group))

(defn submit-and-wait
  ([catalog]
   (submit-and-wait catalog lifecycles))
  ([catalog lc]
   (let [job-info (onyx.api/submit-job
                    peer-config
                    {:catalog catalog
                     :workflow workflow
                     :lifecycles lc
                     :task-scheduler :onyx.task-scheduler/balanced})]
     (onyx.api/await-job-completion peer-config (:job-id job-info))
     job-info)))

(defn run-job
  ([catalog]
   (run-job catalog lifecycles))
  ([catalog lc]
   (submit-and-wait catalog lc)
   (take-segments! out-chan 5000)))

(let [conn (u/connect-rest-client)]
  (esrd/create conn (.toString id) "_default_" {:foo "bar"})
  (Thread/sleep 5000)

  (def res-http-q&map&idx (run-job catalog-http-q&map&idx))
  (def res-http-q&idx (run-job catalog-http-q&idx))
  (def res-http-q&all (run-job catalog-http-q&all))
  (def res-http-idx (run-job catalog-http-idx))
  (def res-native-q&map&idx (run-job catalog-native-q&map&idx))
  (def res-native-q&idx (run-job catalog-native-q&idx))
  (def res-native-q&all (run-job catalog-native-q&all))
  (def res-native-idx (run-job catalog-native-idx))

  (u/delete-indexes (.toString id))
  (doseq [n (range n-messages)]
    (esrd/create conn (.toString id) "_default_" {:foo "bar"} :id (str n)))
  (Thread/sleep 5000)

  (let [job-info-offset (submit-and-wait catalog-http-q&map&idx)]
    (def res-multi (take-segments! out-chan 5000))
    (def task-chunk-offset (extensions/read-chunk (:log env) :chunk (get-in job-info-offset [:task-ids :read-messages :id]))))

  (let [job-info-restart (submit-and-wait (update-in catalog-http-q&map&idx [0] assoc :elasticsearch/restart-on-fail true))]
    (def task-chunk-restart (extensions/read-chunk (:log env) :chunk (get-in job-info-restart [:task-ids :read-messages :id]))))

  (def res-multi-fail (run-job catalog-http-q&map&idx lifecycles-fail))

  (u/delete-indexes (.toString id)))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)

(deftest catalog-http-query
  (testing "Successful Query for HTTP with Query, Map, and Index defined"
    (is (= "bar" (-> res-http-q&map&idx first :_source :foo))))
  (testing "Successful Query for HTTP with Query and Index defined"
    (is (= "bar" (-> res-http-q&idx first :_source :foo))))
  (testing "Successful Query for HTTP with Query defined"
    (is (= "bar" (-> res-http-q&all first :_source :foo))))
  (testing "Successful Query for HTTP for all"
    (is (= "bar" (-> res-http-idx first :_source :foo)))))

(deftest catalog-native-query
  (testing "Successful Query for Native with Query, Map, and Index defined"
    (is (= "bar" (-> res-native-q&map&idx first :_source :foo))))
  (testing "Successful Query for Native with Query and Index defined"
    (is (= "bar" (-> res-native-q&idx first :_source :foo))))
  (testing "Successful Query for Native with Query defined"
    (is (= "bar" (-> res-native-q&all first :_source :foo))))
  (testing "Successful Query for Native for all"
    (is (= "bar" (-> res-native-idx first :_source :foo)))))

(deftest fault-logic
  (testing "Successfully processed all messages no failure"
    (is (= 11 (count res-multi))))
  (testing "Successfully wrote status to log for restart-on-fail=false"
    (is (= :complete (:status task-chunk-offset))))
  (testing "Updates not written to log for restart-on-fail=true"
    (is (= -1 (:chunk-index task-chunk-restart))))
  (testing "Successfully processed all messages with failure"
    (is (= 11 (count res-multi-fail)))))
