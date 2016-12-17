(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
            [jepsen.atlasdb.lock :as lock]
            [jepsen.atlasdb.timestamp :as timestamp]
            [jepsen.core :as jepsen]))

(deftest lock-test
   (is (:valid? (:results (jepsen/run! (lock/lock-test))))))

(deftest timestamp-test
   (is (:valid? (:results (jepsen/run! (timestamp/timestamp-test))))))
