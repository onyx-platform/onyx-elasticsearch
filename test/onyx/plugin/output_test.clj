(ns onyx.plugin.output-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async]
            [onyx.plugin.elasticsearch-output]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def env-config 
  {:onyx/id id
   :zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config 
  {:onyx/id id
   :zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port-range [40200 40260]
   :onyx.messaging/bind-addr "localhost"})

(def env (onyx.api/start-env env-config))

(def peer-group (onyx.api/start-peer-group peer-config))

(def n-messages 100)

(def batch-size 20)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.elasticsearch-output/output
    :onyx/type :output
    :onyx/medium :elasticsearch
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Documentation for your datasink"}])

(def workflow [[:in :out]])

(def in-chan (chan (inc n-messages)))

(def out-datasink (atom []))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(defn inject-out-datasink [event lifecycle]
  {:elasticsearch/example-datasink out-datasink})

(def out-calls
  {:lifecycle/before-task-start inject-out-datasink})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.elasticsearch-output/writer-calls}])

(doseq [n (range n-messages)]
  (>!! in-chan {:n n}))

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

(def results @out-datasink)

(deftest testing-output
  (testing "Output is written correctly"
    (let [expected (set (map (fn [x] {:n x}) (range n-messages)))]
      (is (= expected (set (butlast results))))
      (is (= :done (last results))))))

(doseq [v-peer v-peers]
  (onyx.api/shutdown-peer v-peer))

(onyx.api/shutdown-peer-group peer-group)

(onyx.api/shutdown-env env)
