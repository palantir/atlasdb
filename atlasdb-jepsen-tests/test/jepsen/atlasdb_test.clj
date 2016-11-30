(ns jepsen.atlasdb-test
  (:require [clojure.test :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.atlasdb :as atlasdb]))

(deftest atlasdb-test
   (is (:valid? (:results (jepsen/run! (atlasdb/atlasdb-test))))))
