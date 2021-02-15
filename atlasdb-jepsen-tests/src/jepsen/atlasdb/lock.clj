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
  (:import com.palantir.atlasdb.http.SynchronousLockClient)
  (:import com.palantir.atlasdb.http.AsyncLockClient)
  (:import com.palantir.atlasdb.util.MetricsManagers))

(def lock-names ["alpha" "bravo" "charlie" "delta"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Relevant utility methods, including administration of the token store
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn nullable-to-string    [obj] (if (nil? obj) "" (.toString obj)))
(defn create-token-store    [] (atom {}))
(defn replace-token!        [store key obj] (swap! store assoc key obj))
(defn token-store-key       [client-name lock-name] (str client-name lock-name))

(defn assoc-ok-value
  "Associate the map with ':ok', meaning that the server response was 200.
   Also capture the server response and the query parameters we used."
  [op response query-params]
  (assoc op :type :ok
    :value (nullable-to-string response)
    :query-params (nullable-to-string query-params)))

(defn transform-store!
  "Given an operation and a token store, applies the results of the operation to the token store."
  [token-store op lock-name op-value client-name]
  (let [store-key (token-store-key client-name lock-name)]
    (case (:f op)
      (:lock :refresh)
        (replace-token! token-store store-key op-value)
      :unlock
        (replace-token! token-store store-key nil))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Client creation and invocations (i.e. locking, unlocking refreshing)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn lock-operation-with-timeout
  "Executes an operation on the supplied lock service with a specified timeout. This method returns a hash-map
   indicating the success or failure of the operation, along with any attendant payload.
   Successful requests are associated with a :value, while failed requests are associated with an :error."
  [lock-client op time-limit client-name lock-name token-store]
  (timeout time-limit
    {:outcome :fail :error :timeout}
    (try
      (let [result {:outcome :success}]
        (case (:f op)
          :lock
          (let [response (.lock lock-client client-name lock-name)]
            (assoc result :value response :query-params [client-name lock-name]))
          :unlock
          (let [token (@token-store (str client-name lock-name))
                response (.unlockSingle lock-client token)]
            (assoc result :value response :query-params token))
          :refresh
          (let [token (@token-store (str client-name lock-name))
                response (.refreshSingle lock-client token)]
            (assoc result :value response :query-params token))))
      (catch Exception e {:outcome :fail :error e}))))

(defn create-client
  "Creates an object that implements the client/Client protocol.
   The object defines how you create a lock client, and how to request locks from it. The first call to this
   function will return an invalid object: you should call 'setup' on the returned object to get a valid one."
  [lock-client client-name token-store client-supplier]
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
      "Run an operation on our client with a timeout, and then update the token store."
      (let [lock-name (:value op)
            result (lock-operation-with-timeout lock-client op (* 30 1000) client-name lock-name token-store)]
        (case (:outcome result)
          :fail
          (assoc op :type :fail :error (:error result))
          :success
          (let [result-value (:value result)]
            (do (transform-store! token-store op lock-name result-value client-name)
                (assoc-ok-value op result-value (:query-params result)))))))

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
    (lock-test nem (fn [] (SynchronousLockClient/create (MetricsManagers/createForTests) '("n1" "n2" "n3" "n4" "n5")))))

(defn async-lock-test
  [nem]
    (lock-test nem (fn [] (AsyncLockClient/create (MetricsManagers/createForTests) '("n1" "n2" "n3" "n4" "n5")))))
