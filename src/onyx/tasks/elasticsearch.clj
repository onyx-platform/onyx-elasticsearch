(ns onyx.tasks.elasticsearch
  (:require [schema.core :as s]
            [taoensso.timbre :as log]
            [qbits.spandex :as sp]))

(defn inject-writer
  [{{host :elasticsearch/host
     port :elasticsearch/port} :onyx.core/task-map} _]
  (log/info (str "Creating ElasticSearch http client for " host ":" port))
  {:elasticsearch/connection (sp/client {:hosts [(str "http://" host ":" port)]})})

(def writer-lifecycles
  {:lifecycle/before-task-start inject-writer})

(s/defn output
  [task-name :- s/Keyword opts]
  (println opts)
  {:task {:task-map (merge {:onyx/name task-name
                            :onyx/plugin :onyx.plugin.spandex-elasticsearch/output
                            :onyx/type :output
                            :onyx/medium :elasticsearch
                            :onyx/max-peers 1
                            :onyx/doc "Writes segments to an Elasticsearch cluster."}
                           opts)
          :lifecycles [{:lifecycle/task task-name
                        :lifecycle/calls ::writer-lifecycles}]}})
