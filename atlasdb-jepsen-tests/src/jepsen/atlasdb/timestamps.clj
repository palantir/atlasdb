;;
; Copyright 2017 Palantir Technologies, Inc. All rights reserved.
;
; Licensed under the BSD-3 License (the "License")
; you may not use this file except in compliance with the License.
; You may obtain a copy of the License at
;
; http://opensource.org/licenses/BSD-3-Clause
;
; Unless required by applicable law or agreed to in writing, software
; distributed under the License is distributed on an "AS IS" BASIS,
; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
; See the License for the specific language governing permissions and
; limitations under the License.
;
(ns jepsen.atlasdb.timestamps
  (:require [jepsen.atlasdb.timelock :as timelock]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.os.debian :as debian]
            [jepsen.tests :as tests]
            [jepsen.util :refer [timeout]]
            [knossos.history :as history])
  ;; We can import any Java objects, since Clojure runs on the JVM
  (:import com.palantir.atlasdb.jepsen.utils.EventUtils)
  (:import com.palantir.atlasdb.jepsen.JepsenHistoryCheckers)
  (:import com.palantir.atlasdb.http.TimestampClient))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the set of of operations that you can do with a client
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn read-operation [_ _] {:type :invoke, :f :read-operation, :value nil})

(defn timestamp-range-serialize
  [range]
    (if (nil? range) "" (EventUtils/serializeValue range)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Client creation and invocations (i.e. reading a timestamp)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn create-client
  "Creates an object that implements the client/Client protocol.
   The object defines how you create a timestamp client, and how to request timestamps from it. The first call to this
   function will return an invalid object: you should call 'setup' on the returned object to get a valid one.
  "
  [timestamp-client]
  (reify client/Client
    (setup!
      [this test node]
      "Factory that returns an object implementing client/Client"
      (create-client (TimestampClient/create '("n1" "n2" "n3" "n4" "n5"))))

    (invoke!
      [this test op]
      "Run an operation on our client"
      (case (:f op)
        :read-operation
        (timeout (* 30 1000)
          (assoc op :type :fail :error :timeout)
          (try
            (assoc op :type :ok :value (timestamp-range-serialize (.getFreshTimestamps timestamp-client 33333)))
            (catch Exception e
              (assoc op :type :fail :error (.toString e)))))))

    (teardown! [_ test])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; How to check the validity of a run of the Jepsen test: we hand off to a JepsenHistoryChecker
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def checker
  (reify checker/Checker
    (check [this test model history opts]
      (.checkClojureHistory (JepsenHistoryCheckers/createWithNoopCheckers) history))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the Jepsen test
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn timestamps-test
  [nem]
  (assoc tests/noop-test
    :os debian/os
    :client (create-client nil)
    :nemesis nem
    :generator (->> read-operation
                 (gen/stagger 0.05)
                 (gen/nemesis
                   (gen/seq (cycle [(gen/sleep 9)
                                    {:type :info, :f :start}
                                    (gen/sleep 1)
                                    {:type :info, :f :stop}])))
                 (gen/time-limit 10))
    :db (timelock/create-db)
    :checker checker))
