(ns onyx.util.helper
  (:require [clojure.data.json :as json]
            [org.httpkit.client :as http]
            [clojurewerkz.elastisch.rest :as es]
            [clojurewerkz.elastisch.rest.index :as idx]))

(defn es-cluster-name
  "Returns the name of an ElasticSearch cluster, default returns local cluster name"
  ([]
    (es-cluster-name "127.0.0.1" 9200))
  ([host port]
   (let [{:keys [body error]} @(http/get (str "http://" host ":" port))]
     (if error
       (throw (Exception. "Failed to connect to ElasticSearch cluster.  Please ensure it is runnning locally prior to running tests"))
       (get (json/read-str body) "cluster_name")))))

(defn connect-rest-client
  "Returns a connection to Elastic Search for the http client"
  ([]
   (connect-rest-client "127.0.0.1" 9200))
  ([host port]
   (es/connect (str "http://" host ":" port))))

(defn delete-indexes
  "Deletes all indexes in the local Elastic Search cluster.  Provided as a convenience to clear your cluster
  after running the unit tests.  It is not explicitely called anywhere."
  ([]
    (delete-indexes "127.0.0.1" 9200))
  ([host port]
   (let [conn (connect-rest-client host port)]
     (idx/delete conn))))