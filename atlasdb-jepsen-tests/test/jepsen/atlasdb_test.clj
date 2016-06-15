(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.atlasdb :as atlasdb]))

(deftest atlasdb-test
  (let [test (atlasdb/atlasdb-test)]
    (atlasdb/with-cassandra test
      (is (:valid? (:results (jepsen/run! test)))))))
