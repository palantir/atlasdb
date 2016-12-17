(ns jepsen.atlasdb.lock
  (:require [jepsen.atlasdb.timelock :as timelock]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.generator :as gen]
            [jepsen.tests :as tests]
            [jepsen.os.debian :as debian]
            [jepsen.util :refer [timeout]]
            [knossos.history :as history])
    (:import com.palantir.atlasdb.jepsen.JepsenHistoryChecker)
    (:import com.palantir.atlasdb.http.LockClient))

(def lock-names ["bob" "sarah" "alfred" "shelly"])

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Client creation and invocations (i.e. locking, unlocking refreshing)
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn nullable-to-string    [obj] (if (nil? obj) "" (.toString obj)))
(defn create-dict-from-list [l] (zipmap l (repeat nil)))
(defn create-token-store    [] (atom (create-dict-from-list lock-names)))
(defn replace-token         [store key obj] (swap! store (fn [a] (assoc a key obj))))
(defn create-client
  "Creates an object that implements the client/Client protocol.
   The object defines how you create a timestamp client, and how to request timestamps from it. The first call to this
   function will return an invalid object: you should call 'setup' on the returned object to get a valid one.
  "
  [lock-service, client-name, token-store]
  (reify client/Client
    (setup!
      [this test node]
      "Factory that returns an object implementing client/Client"
        (create-client
          (LockClient/create '("n1" "n2" "n3" "n4" "n5"))
          "client"
          (create-token-store)))

    (invoke!
      [this test op]
      "Run an operation on our client"
      (timeout (* 30 1000)
          (assoc op :type :fail :error :timeout)
          (try
            (let [lock-name (:value op)]
              (case (:f op)
                :lock
                  (let [response (LockClient/lock lock-service client-name lock-name)]
                    (do (replace-token token-store lock-name response)
                        (assoc op :type :ok :value (nullable-to-string response))))
                :unlock
                  (let [token (@token-store lock-name)
                        response (LockClient/unlock lock-service token)]
                    (do (replace-token token-store lock-name nil)
                        (assoc op :type :ok :value (nullable-to-string response))))
                :refresh
                  (let [token (@token-store lock-name)
                        response (LockClient/refresh lock-service token)]
                    (do (replace-token token-store lock-name token)
                        (assoc op :type :ok :value (nullable-to-string response))))))
          (catch Exception e
            (assoc op :type :fail :error (.toString e))))))

    (teardown! [_ test])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; How to check the validity of a run of the Jepsen test: we hand off to JepsenHistoryChecker
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def checker
  (reify checker/Checker
    (check [this test model history opts]
      {:valid? true})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; How to check the validity of a run of the Jepsen test: we hand off to JepsenHistoryChecker
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def generator
  (reify gen/Generator
    (op [generator test process]
      (let [random-lock-name (rand-nth lock-names)]
        (condp < (rand)
          ;; 70% chance of a refresh
          0.30 {:type :invoke   :f :refresh   :value random-lock-name}
          ;; 15% chance of a lock
          0.15 {:type :invoke   :f :lock      :value random-lock-name}
          ;; 15% chance of an unlock
               {:type :invoke   :f :unlock    :value random-lock-name})))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Defining the Jepsen test
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn lock-test
  []
  (assoc tests/noop-test
    :client (create-client nil nil nil)
    :generator (->> generator
                    (gen/stagger 0.1)
                    (gen/clients)
                    (gen/time-limit 30))
    :db (timelock/create-db)
    :checker checker))
