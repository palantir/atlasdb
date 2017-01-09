(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
            [jepsen.atlasdb.timestamp :as timestamp]
            [jepsen.core :as jepsen]))

;; Using Clojure's testing framework, we initiate a run of atlasdb-test
;; The test is successful iff the value for the key ":valid?" is truthy
(deftest timestamp-test
   (is (:valid? (:results (jepsen/run! (timestamp/timestamp-test))))))
