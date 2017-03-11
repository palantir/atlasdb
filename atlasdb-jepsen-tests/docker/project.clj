(defproject jepsen.atlasdb "0.1.0"
  :resource-paths ["resources/atlasdb/atlasdb-jepsen-tests-all.jar"]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [jepsen "0.1.3"]
                 [cheshire "5.6.1"]
                 [clj-http "3.1.0"]])
