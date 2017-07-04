(ns jepsen.atlasdb.lock
  (:require [jepsen.atlasdb.timelock :as timelock]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.tests :as tests]
            [jepsen.os.debian :as debian]
            [jepsen.util :refer [timeout]]
            [knossos.history :as history])
  (:import com.palantir.atlasdb.jepsen.JepsenHistoryCheckers)
  (:import com.palantir.atlasdb.http.JepsenLockClient)
  (:import com.palantir.atlasdb.http.SynchronousLockClient))

(def lock-names ["alpha" "bravo" "charlie" "delta"])
(def version-name "version")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Relevant utility methods, including administration of the token store's simple MVCC protocol
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn nullable-to-string    [obj] (if (nil? obj) "" (.toString obj)))

(defn create-token-store    [] (atom {version-name 0}))
(defn version-bump!
  "Attempts to increment the version key in a token store. Returns the version number if CAS succeded, nil if failed."
  [store]
  (let [current-map @store
        target-version (inc (get current-map version-name))]
    (if (compare-and-set! store current-map (assoc current-map version-name target-version))
      target-version
      nil)))
(defn replace-token!
  "Updates a token in the store, if the version in the store matches our expected version.
  Returns true if and only if the store was successfully updated.
  "
  [store key obj expected-version]
  (loop [current-map @store]
    (if-not (== (get current-map version-name) expected-version)
       false
       (if (compare-and-set! store current-map (assoc current-map key obj))
         true
         (recur @store)))))

(defn assoc-ok-value
  "Associate the map with ':ok', meaning that the server response was 200.
   Also capture the server response and the query parameters we used."
  [op response query-params]
  (assoc op :type :ok
    :value (nullable-to-string response)
    :query-params (nullable-to-string query-params)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Client creation and invocations (i.e. locking, unlocking refreshing)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn create-client
  "Creates an object that implements the client/Client protocol.
   The object defines how you create a lock client, and how to request locks from it. The first call to this
   function will return an invalid object: you should call 'setup' on the returned object to get a valid one.
  "
  [lock-client, client-name, token-store, client-supplier]
  (reify client/Client
    (setup!
      [this test node]
      "Factory that returns an object implementing client/Client"
      (create-client
        (apply client-supplier [])
        (name node)
        (create-token-store)
        client-supplier))

    (invoke!
      [this test op]
      "Run an operation on our client"
      (timeout (* 30 1000)
        (assoc op :type :fail :error :timeout)
        (try
          (let [lock-name (:value op)
                token-store-key (str client-name lock-name)
                current-store-version (version-bump! token-store)]
            (if ((complement nil?) current-store-version)
              (case (:f op)
                :lock
                (let [response (.lock lock-client client-name lock-name)]
                  (do (replace-token! token-store token-store-key response current-store-version)
                    (assoc-ok-value op response [client-name lock-name])))
                :unlock
                (let [token (@token-store token-store-key)
                      response (.unlockSingle lock-client token)]
                  (do (replace-token! token-store token-store-key nil current-store-version)
                    (assoc-ok-value op response token)))
                :refresh
                (let [token (@token-store token-store-key)
                      response (.refreshSingle lock-client token)]
                  (do (replace-token! token-store token-store-key token current-store-version)
                    (assoc-ok-value op response token))))))
          (catch Exception e
            (assoc op :type :fail :error (.toString e))))))

    (teardown! [_ test])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; How to check the validity of a run of the Jepsen test.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def checker
  (reify checker/Checker
    (check [this test model history opts]
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
  [nem lock-client-supplier]
  (assoc tests/noop-test
    :client (create-client nil nil nil lock-client-supplier)
    :nemesis nem
    :generator (->> generator
                 (gen/stagger 0.05)
                 (gen/nemesis
                   (gen/seq (cycle [(gen/sleep 5)
                                    {:type :info, :f :start}
                                    (gen/sleep 85)
                                    {:type :info, :f :stop}])))
                 (gen/time-limit 360))
    :db (timelock/create-db)
    :checker checker))

(defn sync-lock-test
  [nem]
    (lock-test nem (fn [] (SynchronousLockClient/create '("n1" "n2" "n3" "n4" "n5")))))
