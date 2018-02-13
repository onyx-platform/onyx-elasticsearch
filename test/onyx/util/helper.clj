;; (ns onyx.util.helper
;;   (:require [clojure.data.json :as json]
;;             [org.httpkit.client :as http]
;;             [clojurewerkz.elastisch.rest :as es]
;;             [clojurewerkz.elastisch.rest.index :as idx]))

;; (defn es-cluster-name
;;   "Returns the name of an ElasticSearch cluster, default returns local cluster name"
;;   ([]
;;     (es-cluster-name "127.0.0.1" 9200))
;;   ([host port]
;;    (let [{:keys [body error]} @(http/get (str "http://" host ":" port) {:timeout 5000})]
;;      (if error
;;        (throw (Exception. "Failed to connect to ElasticSearch cluster.  Please ensure it is runnning locally prior to running tests"))
;;        (get (json/read-str body) "cluster_name")))))

;; (defn connect-rest-client
;;   "Returns a connection to Elastic Search for the http client"
;;   ([]
;;    (connect-rest-client "127.0.0.1" 9200))
;;   ([host port]
;;    (es/connect (str "http://" host ":" port))))

;; (defn delete-indexes
;;   "Deletes the specified index.  If no index is provided, will delete all indexes.
;;   Optionally can specify the cluster host and port.  If not, will default to local.
;;   Used to clean up after testing."
;;   ([]
;;     (delete-indexes "127.0.0.1" 9200))
;;   ([idx]
;;    (delete-indexes "127.0.0.1" 9200 idx))
;;   ([host port]
;;    (let [conn (connect-rest-client host port)]
;;      (idx/delete conn)))
;;   ([host port idx]
;;    (let [conn (connect-rest-client host port)]
;;      (idx/delete conn idx))))
