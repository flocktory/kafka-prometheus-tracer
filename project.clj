(defproject com.flocktory.component/kafka-prometheus-tracer "0.0.1"
  :description "Prometheus tracer for kafka consumer"
  :url "https://github.com/flocktory/kafka-prometheus-tracer"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[com.flocktory/kafka-consumer "0.0.5"]
                 [com.stuartsierra/component "0.3.2"]
                 [io.prometheus/simpleclient "0.1.0"]
                 [io.prometheus/simpleclient_common "0.1.0"]
                 [io.prometheus/simpleclient_hotspot "0.1.0"]]
  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :sign-releases false}]])
