(ns onyx.plugin.input-test-spandex
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.plugin.spandex-elasticsearch :as e]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq]
            [onyx.job :refer [add-task]]
            [onyx.tasks.core-async]
            [onyx.tasks.seq]
            [onyx.tasks.null]
            [onyx.tasks.function]
            [onyx.static.uuid :refer [random-uuid]]
            [onyx.test-helper :refer [with-test-env]]))

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

(defn windowed-task
  [task-name opts sync-fn]
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/type :function
                            :onyx/fn :clojure.core/identity
                            :onyx/max-peers 1}
                           opts)
          :windows [{:window/id :collect-segments
                     :window/task task-name
                     :window/type :global
                     :window/aggregation :onyx.windowing.aggregation/conj}]
          :triggers [{:trigger/window-id :collect-segments
                      :trigger/fire-all-extents? true
                      :trigger/on :onyx.trigger/segment
                      :trigger/threshold [1 :elements]
                      :trigger/sync sync-fn}]}})

(defn build-job [workflow compact-job task-scheduler]
  (reduce (fn [job {:keys [name task-opts type args] :as task}]
            (case type
              :seq (add-task job (apply onyx.tasks.seq/input-serialized name task-opts (:input task) args))
              :windowed (add-task job (apply windowed-task name task-opts args))
              :fn (add-task job (apply onyx.tasks.function/function name task-opts args))
              :null-out (add-task job (apply onyx.tasks.null/output name task-opts args))
              :async-out (add-task job (apply onyx.tasks.core-async/output name task-opts (:chan-size task) args))))
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

(defn my-inc [{:keys [n] :as segment}]
  (assoc segment :n (inc n)))

(deftest abs-plugin-test
  (let [n-messages 100
        task-opts {:onyx/batch-size 20}
        job (build-job [[:in :inc] [:inc :out]] 
                       [{:name :in
                         :type :seq 
                         :task-opts task-opts 
                         :input (map (fn [n] {:n n}) (range n-messages))}
                        {:name :inc
                         :type :fn 
                         :task-opts (assoc task-opts :onyx/fn ::my-inc)}
                        {:name :out
                         :type :async-out
                         :chan-size 10000000
                         :task-opts task-opts}]
                       :onyx.task-scheduler/balanced)
        output (run-test-job job 3)]
    (is (= (set (map (fn [x] {:n (inc x)}) (range n-messages)))
           (set (:out output))))))
