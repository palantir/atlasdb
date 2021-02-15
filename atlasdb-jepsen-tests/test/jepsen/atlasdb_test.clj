(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
            [jepsen.nemesis :as nemesis]
            [jepsen.atlasdb.lock :as lock]
            [jepsen.atlasdb.timelock :as timelock]
            [jepsen.atlasdb.timestamp :as timestamp]
            [jepsen.core :as jepsen]))

;; Using Clojure's testing framework, we initiate our test runs
;; Tests successful iff the value for the key ":valid?" is truthy

; (deftest async-lock-test-crash
;   (is (:valid? (:results (jepsen/run! (lock/async-lock-test timelock/crash-nemesis))))))
;
; (deftest async-lock-test-partition
;   (is (:valid? (:results (jepsen/run! (lock/async-lock-test (nemesis/partition-random-halves)))))))
;
; (deftest sync-lock-test-crash
;   (is (:valid? (:results (jepsen/run! (lock/sync-lock-test timelock/crash-nemesis))))))
;
; (deftest sync-lock-test-partition
;   (is (:valid? (:results (jepsen/run! (lock/sync-lock-test (nemesis/partition-random-halves)))))))

(deftest timestamp-test-crash
  (is (:valid? (:results (jepsen/run! (timestamp/timestamp-test timelock/crash-nemesis))))))

(deftest timestamp-test-partition
  (is (:valid? (:results (jepsen/run! (timestamp/timestamp-test (nemesis/partition-random-halves)))))))
