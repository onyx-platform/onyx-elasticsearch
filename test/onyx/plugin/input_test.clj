(ns onyx.plugin.input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.elasticsearch]
            [onyx.api]
            [onyx.util.helper :as u]
            [clojurewerkz.elastisch.rest.document :as esrd]))

;; ElasticSearch should be running locally on standard ports
;; (http: 9200, native: 9300) prior to running the tests.

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
   :onyx.messaging/peer-port-range [40200 40260]
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 1)

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

(def lifecycles
  [{:lifecycle/task :read-messages
    :lifecycle/calls :onyx.plugin.elasticsearch/read-messages-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

;; Place message in ElasticSearch index
(let [conn (u/connect-rest-client)]
  (esrd/create conn (.toString id) "_default_" {:foo "bar"}))
(Thread/sleep 5000)

(def v-peers (onyx.api/start-peers 2 peer-group))

(defn run-job
  [catalog]
  (let [job-info (onyx.api/submit-job
                   peer-config
                   {:catalog catalog
                    :workflow workflow
                    :lifecycles lifecycles
                    :task-scheduler :onyx.task-scheduler/balanced})]
    (onyx.api/await-job-completion peer-config (:job-id job-info))
    (take-segments! out-chan 5000)))

(def res-http-q&map&idx (run-job catalog-http-q&map&idx))

(def res-http-q&idx (run-job catalog-http-q&idx))

(def res-http-q&all (run-job catalog-http-q&all))

(def res-http-idx (run-job catalog-http-idx))

(def res-native-q&map&idx (run-job catalog-native-q&map&idx))

(def res-native-q&idx (run-job catalog-native-q&idx))

(def res-native-q&all (run-job catalog-native-q&all))

(def res-native-idx (run-job catalog-native-idx))

(use-fixtures
  :once (fn [f]
          (f)
          (u/delete-indexes (.toString id))))

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

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)