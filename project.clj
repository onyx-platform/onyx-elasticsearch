(defproject org.onyxplatform/onyx-elasticsearch "0.7.14.0"
  :description "Onyx plugin for Elasticsearch"
  :url "https://github.com/manderson202/onyx-elasticsearch"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.onyxplatform/onyx "0.7.14"]
                 [clojurewerkz/elastisch "2.2.0-beta4"]
                 [com.taoensso/timbre "4.1.4"]]
  :profiles {:dev {:dependencies [[http-kit "2.1.19"]
                                  [org.clojure/data.json "0.2.6"]]}})
