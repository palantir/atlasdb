(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.atlasdb :as atlasdb]))

;; Using Clojure's testing framework, we initiate a run of atlasdb-test
;; The test is successful iff the value for the key ":valid?" is truthy
(deftest atlasdb-test
   (is (:valid? (:results (jepsen/run! (atlasdb/atlasdb-test))))))
