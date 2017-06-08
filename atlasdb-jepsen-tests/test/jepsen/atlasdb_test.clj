(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
            [jepsen.nemesis :as nemesis]
            [jepsen.atlasdb.lock :as lock]
            [jepsen.atlasdb.timelock :as timelock]
            [jepsen.atlasdb.timestamp :as timestamp]
            [jepsen.core :as jepsen]))

;; Using Clojure's testing framework, we initiate our test runs
;; Tests successful iff the value for the key ":valid?" is truthy

(deftest timestamp-test-crash
  (is (:valid? (:results (jepsen/run! (timestamp/timestamp-test timelock/crash-nemesis))))))

(deftest timestamp-test-partition
  (is (:valid? (:results (jepsen/run! (timestamp/timestamp-test nemesis/partition-random-halves))))))

(deftest lock-test-crash
  (is (:valid? (:results (jepsen/run! (lock/lock-test timelock/crash-nemesis))))))

(deftest lock-test-partition
  (is (:valid? (:results (jepsen/run! (lock/lock-test nemesis/partition-random-halves))))))
