;; (ns onyx.plugin.input-test
;;   (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer timeout alts!!]]
;;             [clojure.test :refer [deftest is testing use-fixtures]]
;;             [onyx.test-helper :refer [with-test-env]]
;;             [taoensso.timbre :refer [info]]
;;             [onyx.extensions :as extensions]
;;             [onyx.plugin.core-async :refer [take-segments!]]
;;             [onyx.plugin.elasticsearch]
;;             [onyx.api]
;;             [onyx.util.helper :as u]
;;             [clojurewerkz.elastisch.rest.document :as esrd]))

;; ;; ElasticSearch should be running locally on standard ports
;; ;; (http: 9200, native: 9300) prior to running these tests.

;; (def id (java.util.UUID/randomUUID))

;; (def zk-addr "127.0.0.1:2188")

;; (def es-host "127.0.0.1")

;; (def es-rest-port 9200)

;; (def es-native-port 9300)

;; (def env-config
;;   {:onyx/tenancy-id id
;;    :zookeeper/address zk-addr
;;    :zookeeper/server? true
;;    :zookeeper.server/port 2188})

;; (def peer-config
;;   {:onyx/tenancy-id id
;;    :zookeeper/address zk-addr
;;    :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
;;    :onyx.messaging.aeron/embedded-driver? true
;;    :onyx.messaging/allow-short-circuit? false
;;    :onyx.messaging/impl :aeron
;;    :onyx.messaging/peer-port 40200
;;    :onyx.messaging/bind-addr "localhost"})

;; (def n-messages 10)

;; (def batch-size 20)

;; (def workflow [[:read-messages :out]])

;; ;; Create catalogs with different search scenarios

;; (def catalog-base
;;   [{:onyx/name :read-messages
;;     :onyx/plugin :onyx.plugin.elasticsearch/read-messages
;;     :onyx/type :input
;;     :onyx/medium :elasticsearch
;;     :elasticsearch/host es-host
;;     :elasticsearch/cluster-name (u/es-cluster-name)
;;     :elasticsearch/http-ops {}
;;     :onyx/batch-size 1
;;     :onyx/max-peers 1
;;     :onyx/doc "Read messages from an ElasticSearch Query"}

;;    {:onyx/name :out
;;     :onyx/plugin :onyx.plugin.core-async/output
;;     :onyx/type :output
;;     :onyx/medium :core.async
;;     :onyx/batch-size batch-size
;;     :onyx/max-peers 1
;;     :onyx/doc "Writes segments to a core.async channel"}])

;; (def catalog-http-q&map&idx
;;   (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
;;                                      :elasticsearch/client-type :http
;;                                      :elasticsearch/index (.toString id)
;;                                      :elasticsearch/mapping "_default_"
;;                                      :elasticsearch/query {:term {:foo "bar"}}}))

;; (def catalog-http-q&idx
;;   (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
;;                                      :elasticsearch/client-type :http
;;                                      :elasticsearch/index (.toString id)
;;                                      :elasticsearch/query {:term {:foo "bar"}}}))

;; (def catalog-http-q&all
;;   (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
;;                                      :elasticsearch/client-type :http
;;                                      :elasticsearch/query {:term {:foo "bar"}}}))

;; (def catalog-http-idx
;;   (update-in catalog-base [0] merge {:elasticsearch/port es-rest-port
;;                                      :elasticsearch/index (.toString id)
;;                                      :elasticsearch/client-type :http}))

;; (def catalog-native-q&map&idx
;;   (update-in catalog-http-q&map&idx [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

;; (def catalog-native-q&idx
;;   (update-in catalog-http-q&idx [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

;; (def catalog-native-q&all
;;   (update-in catalog-http-q&all [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

;; (def catalog-native-idx
;;   (update-in catalog-http-idx [0] assoc :elasticsearch/client-type :native :elasticsearch/port es-native-port))

;; (def out-chan (atom nil))

;; (defn inject-out-ch [event lifecycle]
;;   {:core.async/chan @out-chan})

;; (def out-calls
;;   {:lifecycle/before-task-start inject-out-ch})

;; (def batch-num (atom 0))

;; (def read-crash
;;   {:lifecycle/before-batch
;;    (fn [event lifecycle]
;;      ;; give the peer a bit of time to write the chunks out and ack the batches,
;;      ;; since we want to ensure that the batches aren't re-read on restart for ease of testing
;;      (Thread/sleep 7000)
;;      (when (= (swap! batch-num inc) 2)
;;        (throw (ex-info "Restartable" {:restartable? true})))
;;      {})
;;    :lifecycle/handle-exception (constantly :restart)})

;; (def lifecycles
;;   [{:lifecycle/task :read-messages
;;     :lifecycle/calls :onyx.plugin.elasticsearch/read-messages-calls}
;;    {:lifecycle/task :out
;;     :lifecycle/calls ::out-calls}
;;    {:lifecycle/task :out
;;     :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

;; (def lifecycles-fail
;;   [{:lifecycle/task :read-messages
;;     :lifecycle/calls :onyx.plugin.elasticsearch/read-messages-calls}
;;    {:lifecycle/task :read-messages
;;     :lifecycle/calls ::read-crash}
;;    {:lifecycle/task :out
;;     :lifecycle/calls ::out-calls}
;;    {:lifecycle/task :out
;;     :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

;; (deftest native-test
;;   (def conn (u/connect-rest-client))
;;   (esrd/create conn (.toString id) "_default_" {:foo "bar"})
;;   (Thread/sleep 5000)

;;   (testing "Successful Query for Native with Query and Index defined"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-native-q&idx
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= "bar" (-> result first :_source :foo)))))))


;;   (testing "Successful Query for Native with Query, Map, and Index defined"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-native-q&map&idx
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= "bar" (-> result first :_source :foo)))))))

;;   (testing "Successful Query for Native with Query defined"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-native-q&all
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= "bar" (-> result first :_source :foo)))))))

;;   (testing "Successful Query for Native for all"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-native-idx
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= "bar" (-> result first :_source :foo))))))))

;; (deftest http-test
;;   (def conn (u/connect-rest-client))
;;   (esrd/create conn (.toString id) "_default_" {:foo "bar"})
;;   (Thread/sleep 5000)

;;   (testing "Successful Query for HTTP with Query, Map, and Index defined"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-http-q&map&idx
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= "bar" (-> result first :_source :foo)))))))

;;     (testing "Successful Query for HTTP with Query and Index defined"
;;       (with-test-env [test-env [2 env-config peer-config]]
;;         (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;         (let [job {:catalog catalog-http-q&idx
;;                    :workflow workflow
;;                    :lifecycles lifecycles
;;                    :task-scheduler :onyx.task-scheduler/balanced}
;;               {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;           (onyx.api/await-job-completion peer-config job-id)
;;           (let [result (take-segments! @out-chan 5000)]
;;             (is (= "bar" (-> result first :_source :foo)))))))

;;   (testing "Successful Query for HTTP with Query defined"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-http-q&all
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= "bar" (-> result first :_source :foo)))))))

;;   (testing "Successful Query for HTTP for all"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-http-idx
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= "bar" (-> result first :_source :foo))))))))

;; (deftest fault-logic
;;   (u/delete-indexes (.toString id))
;;   (doseq [n (range n-messages)]
;;     (esrd/create conn (.toString id) "_default_" {:foo "bar"} :id (str n)))
;;   (Thread/sleep 5000)

;;   (testing "Successfully processed all messages no failure"
;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-http-q&map&idx
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)
;;               task-chunk-offset (extensions/read-chunk (:log (:env test-env)) :chunk (str job-id "#" :read-messages))]
;;           (is (= 11 (count result)))
;;           (is (= :complete (:status task-chunk-offset))))))

;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog (update-in catalog-http-q&map&idx [0] assoc :elasticsearch/restart-on-fail true)
;;                  :workflow workflow
;;                  :lifecycles lifecycles
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [task-chunk-restart (extensions/read-chunk (:log (:env test-env)) :chunk (str job-id "#" :read-messages))]
;;           (is (= -1 (:chunk-index task-chunk-restart))))))

;;     (with-test-env [test-env [2 env-config peer-config]]
;;       (reset! out-chan (chan (sliding-buffer (inc n-messages))))
;;       (let [job {:catalog catalog-http-q&map&idx
;;                  :workflow workflow
;;                  :lifecycles lifecycles-fail
;;                  :task-scheduler :onyx.task-scheduler/balanced}
;;             {:keys [job-id]} (onyx.api/submit-job peer-config job)]
;;         (onyx.api/await-job-completion peer-config job-id)
;;         (let [result (take-segments! @out-chan 5000)]
;;           (is (= 11 (count result))))))))
