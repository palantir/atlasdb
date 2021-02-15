(ns jepsen.atlasdb.timestamp
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
  (:import com.palantir.atlasdb.http.TimestampClient)
  (:import com.palantir.atlasdb.jepsen.JepsenHistoryCheckers)
  (:import com.palantir.atlasdb.util.MetricsManagers))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the set of of operations that you can do with a client
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn read-operation [_ _] {:type :invoke, :f :read-operation, :value nil})

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
      (create-client (TimestampClient/create (MetricsManagers/createForTests) '("n1" "n2" "n3" "n4" "n5"))))

    (invoke!
      [this test op]
      "Run an operation on our client"
      (case (:f op)
        :read-operation
        (timeout (* 30 1000)
          (assoc op :type :fail :error :timeout)
          (try
            (assoc op :type :ok :value (.getFreshTimestamp timestamp-client))
            (catch Exception e
              (assoc op :type :fail :error (.toString e)))))))

    (teardown! [_ test])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; How to check the validity of a run of the Jepsen test: we hand off to a JepsenHistoryChecker
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def checker
  (reify checker/Checker
    (check [this test model history opts]
      (.checkClojureHistory (JepsenHistoryCheckers/createWithTimestampCheckers) history))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the Jepsen test
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn timestamp-test
  [nem]
  (assoc tests/noop-test
    :os debian/os
    :client (create-client nil)
    :nemesis nem
    :generator (->> read-operation
                 (gen/stagger 0.05)
                 (gen/nemesis
                   (gen/seq (cycle [(gen/sleep 1)])))
                 (gen/time-limit 360))
    :db (timelock/create-db)
    :checker checker))
