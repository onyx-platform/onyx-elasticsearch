(ns onyx.plugin.output-test-spandex
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [onyx.plugin.spandex-elasticsearch :as e]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq]
            [onyx.job :refer [add-task]]
            [onyx.tasks.core-async]
            [onyx.tasks.seq]
            [onyx.tasks.null]
            [onyx.tasks.function]
            [onyx.tasks.elasticsearch]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [with-test-env]]
            [qbits.spandex :as spdx]))

(def id (java.util.UUID/randomUUID))

(def zk-addr "127.0.0.1:2188")

(def es-host "127.0.0.1")

(def es-rest-port 9200)

(def es-native-port 9300)

(def env-config
  {:onyx/tenancy-id id
   :zookeeper/address zk-addr
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config
  {:onyx/tenancy-id id
   :zookeeper/address zk-addr
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def n-messages 7)

(def batch-size 20)

(def catalog-base
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
    :elasticsearch/host es-host
    :elasticsearch/index id
    :elasticsearch/mapping "_default_"
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes documents to elasticsearch"}])

(defn build-job [workflow compact-job task-scheduler]
  (reduce (fn [job {:keys [name task-opts type args] :as task}]
            (case type
              :seq (add-task job (apply onyx.tasks.seq/input-serialized name task-opts (:input task) args))
              :elastic (add-task job (apply onyx.tasks.elasticsearch/output name task-opts args))
              :fn (add-task job (apply onyx.tasks.function/function name task-opts args))))
          {:workflow workflow
           :catalog []
           :lifecycles []
           :triggers []
           :windows []
           :task-scheduler task-scheduler}
          compact-job))

(defn run-test-job [job n-peers]
  (let [id (random-uuid)
        env-config env-config
        peer-config peer-config]
    (with-test-env [test-env [n-peers env-config peer-config]]
      (let [{:keys [job-id]} (onyx.api/submit-job peer-config job)
            _ (assert job-id "Job was not successfully submitted")
            _ (onyx.test-helper/feedback-exception! peer-config job-id)
            out-channels (onyx.plugin.core-async/get-core-async-channels job)]
        (into {} 
              (map (fn [[k chan]]
                     [k (take-segments! chan 50)])
                   out-channels))))))

(defn get-document-rest-request 
  [doc-id name]
  {:elasticsearch/index :get_together
   :elasticsearch/mapping-type :group
   :elasticsearch/write-type :index
   :elasticsearch/id doc-id
   :elasticsearch/message {:name name}})

(def es-host "127.0.0.1")

(def es-rest-port 9200)

(def write-elastic-opts
  {:elasticsearch/host es-host
   :elasticsearch/port es-rest-port})

(def test-index :output_test)

(def test-set
  (map get-document-rest-request (range 4) ["John" "Maria" "Peter" "Sasquatch"]))

(def test-set2
  [{:elasticsearch/message {:name "http:insert_detail-msg_noid" :index "one"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index}
   {:elasticsearch/message {:name "http:insert_detail-msg_id"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index
    :elasticsearch/id "1"}
   {:elasticsearch/message {:name "http:insert_detail-msg_id" :new "new"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :update
    :elasticsearch/id "1"}
   {:elasticsearch/message {:name "http:insert_detail-msg_id"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :upsert
    :elasticsearch/id "2"}
   {:elasticsearch/message {:name "http:insert-to-be-deleted"}
    :elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :index
    :elasticsearch/id "3"}
   {:elasticsearch/index test-index
    :elasticsearch/mapping-type :group
    :elasticsearch/write-type :delete
    :elasticsearch/id "3"}])

(defn index-documents []
  (let [n-messages 5
        task-opts {:onyx/batch-size 20}
        job (build-job [[:in :write-elastic]]
                       [{:name :in
                         :type :seq 
                         :task-opts task-opts 
                         :input test-set2} 
                        {:name :write-elastic
                         :type :elastic
                         :task-opts (merge task-opts write-elastic-opts)}]
                       :onyx.task-scheduler/balanced)]
    (run-test-job job 3)
    ; Wait for Elasticsearch to update
    (Thread/sleep 7000)))

(defn delete-index [index]
  (let [client (spdx/client {:hosts [(str "http://" es-host ":" es-rest-port)]})]
    (spdx/request client {:url [index] :method :delete})))

(defn- search [client body]
  (spdx/request client {:url [test-index :group :_search] :method :get :body body}))

(use-fixtures :once (fn [f]
                      (index-documents)
                      (f)
                      (delete-index test-index)))

(let [client (spdx/client {:hosts [(str "http://" es-host ":" es-rest-port)]})]

  (deftest check-http&write-job
    (testing "Insert: plain message with no id defined"
      (let [{:keys [:body]} (search client {:query {:match {:index "one"}}})]
        (is (= 1 (get-in body [:hits :total])))
        (is (not-empty (first (get-in body [:hits :hits]))))))
    (let [{:keys [:body]} (search client {:query {:match {:_id "1"}}})]
        (testing "Insert: detail message with id defined"
          (is (= 1 (get-in body [:hits :total]))))
        (testing "Update: detail message with id defined"
          (is (= "new" (get-in (first (get-in body [:hits :hits])) [:_source :new])))))
    (testing "Upsert: detail message with id defined"
      (let [{:keys [:body]} (search client {:query {:match {:_id "2"}}})]
        (is (= 1 (get-in body [:hits :total])))))
    (testing "Delete: detail defined"
      (let [{:keys [:body]} (search client {:query {:match {:_id "3"}}})]
        (is (= 0 (get-in body [:hits :total])))))))
