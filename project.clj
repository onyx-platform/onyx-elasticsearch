(defproject org.onyxplatform/onyx-elasticsearch "0.8.7.0-SNAPSHOT"
  :description "Onyx plugin for Elasticsearch"
  :url "https://github.com/onyx-platform/onyx-elasticsearch"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.8.7-20160129_123916-g91fe3c8"]
                 [clojurewerkz/elastisch "2.2.0"]]
  :profiles {:dev {:dependencies [[http-kit "2.1.19"]
                                  [org.clojure/data.json "0.2.6"]]
                   :plugins [[lein-set-version "0.4.1"]
                             [lein-update-dependency "0.1.2"]
                             [lein-pprint "1.1.1"]]}
             :circle-ci {:jvm-opts ["-Xmx4g"]}})
