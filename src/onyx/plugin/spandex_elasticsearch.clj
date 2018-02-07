(ns onyx.plugin.spandex-elasticsearch
  (:require [onyx.plugin.protocols :as p]
            [taoensso.timbre :as log]
            [qbits.spandex :as sp]
            [schema.core :as s]))

(defn rest-method
  [write-type]
  (case write-type
    :create :put
    :update :post
    :delete :delete
    (throw (Exception. (str "Invalid write type: " write-type)))))

(def elastic-write-request {:elasticsearch/host s/Str
                            :elasticsearch/port s/Num
                            :elasticsearch/index s/Keyword
                            :elasticsearch/mapping-type s/Keyword
                            :elasticsearch/write-type s/Keyword
                            (s/optional-key :elasticsearch/id) s/Any
                            :elasticsearch/message {}})

(s/defn rest-request
  "Takes in a settings map and returns a REST request to send to the spandex client."
  [options]
  (s/validate elastic-write-request options)
  (let [{:keys [:elasticsearch/host
                :elasticsearch/port
                :elasticsearch/index
                :elasticsearch/mapping-type
                :elasticsearch/write-type
                :elasticsearch/id
                :elasticsearch/message]} options]
    {:url (cond-> [index mapping-type] (some? id) (conj id))
     :method (rest-method write-type)
     :body message}))

(defrecord ElasticSearchWriter []
  p/Plugin
  (start [this event] this) 
  (stop [this event] this)

  p/Checkpointed
  (recover! [this replica-version checkpoint])
  (checkpoint [this])
  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)
  (completed? [this] true)

  p/Output
  (prepare-batch [this event replica messenger] 
    true)
  (write-batch 
    [this {:keys [onyx.core/write-batch elasticsearch/connection]} replica messenger]
    (doseq [event write-batch]
      (sp/request connection event))
    true))

(defn output [event]
  (->ElasticSearchWriter))
