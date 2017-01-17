(ns jepsen.atlasdb.lock
  (:require [jepsen.atlasdb.timelock :as timelock]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]
            [jepsen.os.debian :as debian]
            [jepsen.util :refer [timeout]]
            [knossos.history :as history]
            [clojure.data.json :as json]
            [clojure.java.io :refer [writer]])
  (:import com.palantir.atlasdb.jepsen.JepsenHistoryCheckers)
  (:import com.palantir.atlasdb.http.LockClient))

(def lock-names ["bob" "sarah" "alfred" "shelly"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Client creation and invocations (i.e. locking, unlocking refreshing)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn nullable-to-string    [obj] (if (nil? obj) "" (.toString obj)))
(defn create-token-store    [] (atom {}))
(defn replace-token         [store key obj] (swap! store assoc key obj))
(defn assoc-ok-value
  "Associate the map with ':ok', meaning that the server response was 200.
   Also capture the server response and the query parameters we used."
  [op response query-params]
  (assoc op :type :ok
            :value (nullable-to-string response)
            :query-params (nullable-to-string query-params)))

(defn create-client
  "Creates an object that implements the client/Client protocol.
   The object defines how you create a lock client, and how to request locks from it. The first call to this
   function will return an invalid object: you should call 'setup' on the returned object to get a valid one.
  "
  [lock-service, client-name, token-store]
  (reify client/Client
    (setup!
      [this test node]
      "Factory that returns an object implementing client/Client"
      (create-client
        (LockClient/create '("n1" "n2" "n3" "n4" "n5"))
        (name node)
        (create-token-store)))

    (invoke!
      [this test op]
      "Run an operation on our client"
      (timeout (* 30 1000)
        (assoc op :type :fail :error :timeout)
        (try
          (let [lock-name (:value op)
                token-store-key (str client-name lock-name)]
            (case (:f op)
              :lock
                (let [response (LockClient/lock lock-service client-name lock-name)]
                  (do (replace-token token-store token-store-key response)
                      (assoc-ok-value op response [client-name lock-name])))
              :unlock
                (let [token (@token-store token-store-key)
                      response (LockClient/unlock lock-service token)]
                  (do (replace-token token-store token-store-key nil)
                      (assoc-ok-value op response token)))
              :refresh
                (let [token (@token-store token-store-key)
                      response (LockClient/refresh lock-service token)]
                  (do (replace-token token-store token-store-key token)
                      (assoc-ok-value op response token)))))
        (catch Exception e
          (assoc op :type :fail :error (.toString e))))))

    (teardown! [_ test])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; How to check the validity of a run of the Jepsen test. Hard-coded to succeed for now.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def checker
  (reify checker/Checker
    (check [this test model history opts]
      (with-open [wrtr (writer "history.json")]
        (json/write history wrtr))
      (.checkClojureHistory (JepsenHistoryCheckers/createWithLockCheckers) history))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; How to generate test events. We randomly mix refreshes, locks, and unlocks.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def generator
  (reify gen/Generator
    (op [generator test process]
      (let [random-lock-name (rand-nth lock-names)]
        (condp > (rand)
          ;; 70% chance of a refresh
          0.70 {:type :invoke   :f :refresh   :value random-lock-name}
          ;; 15% chance of a lock
          0.85 {:type :invoke   :f :lock      :value random-lock-name}
          ;; 15% chance of an unlock
               {:type :invoke   :f :unlock    :value random-lock-name})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the Jepsen test
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn lock-test
  []
  (assoc tests/noop-test
    :client (create-client nil nil nil)
    :nemesis (nemesis/partition-random-halves)
    :generator (->> generator
                    (gen/stagger 0.1)
                    (gen/nemesis
                    (gen/seq (cycle [(gen/sleep 5)
                                     {:type :info, :f :start}
                                     (gen/sleep 15)
                                     {:type :info, :f :stop}])))
                    (gen/time-limit 360))
    :db (timelock/create-db)
    :checker checker))
