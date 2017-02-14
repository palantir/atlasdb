(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
;            [jepsen.atlasdb.lock :as lock]
;            [jepsen.atlasdb.timestamp :as timestamp]
            [jepsen.atlasdb :as test]
            [jepsen.core :as jepsen]))

;; Using Clojure's testing framework, we initiate our test runs
;; Tests successful iff the value for the key ":valid?" is truthy

;(deftest timestamp-test
;   (is (:valid? (:results (jepsen/run! (timestamp/timestamp-test))))))
;
;(deftest lock-test
;   (is (:valid? (:results (jepsen/run! (lock/lock-test))))))

(deftest cassandra-test
  (is (:valid? (:results (jepsen/run! (test/atlasdb-test))))))
