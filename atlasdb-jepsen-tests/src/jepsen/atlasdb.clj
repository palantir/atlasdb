(ns jepsen.atlasdb
  (:require [clj-http.client :as http]
            [clojure.tools.logging :refer :all]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.os.debian :as debian]
            [jepsen.util :refer [timeout]]
            [knossos.history :as history]
            [jepsen.tests :as tests])
  (:import com.palantir.atlasdb.jepsen.JepsenHistoryChecker)
  (:import com.palantir.atlasdb.jepsen.TimestampClient))

(defn create-server
  "Creates an object that implements the db/DB protocol.
   This object defines how to setup and teardown a timelock server on a given
   node, and specifies where the log files can be found.
  "
  []
  (reify db/DB
    (setup! [_ _ node]
      (c/su
        (debian/install-jdk8!)
        (info node "Uploading and unpacking timelock server")
        (c/upload "resources/atlasdb/atlasdb-timelock-server.tgz" "/")
        (c/exec :mkdir "/atlasdb-timelock-server")
        (c/exec :tar :xf "/atlasdb-timelock-server.tgz" "-C" "/atlasdb-timelock-server" "--strip-components" "1")
        (c/upload "resources/atlasdb/timelock.yml" "/atlasdb-timelock-server/var/conf")
        (c/exec :sed :-i (format "s/<HOSTNAME>/%s/" (name node)) "/atlasdb-timelock-server/var/conf/timelock.yml")
        (info node "Starting timelock server")
        (c/exec "source" "/etc/profile" "&&" "/atlasdb-timelock-server/service/bin/init.sh" "start")
        (info node "Waiting until timelock cluster is ready")
        (TimestampClient/waitUntilHostReady (name node))
        (TimestampClient/waitUntilTimestampClusterReady '("n1" "n2" "n3" "n4" "n5"))))

    (teardown! [_ _ node]
      (c/su
        (try (c/exec "/atlasdb-timelock-server/service/bin/init.sh" "stop") (catch Exception _))
        (try (c/exec :rm :-rf "/atlasdb-timelock-server") (catch Exception _))
        (try (c/exec :rm :-f "/atlasdb-timelock-server.tgz") (catch Exception _))))

    db/LogFiles
    (log-files [_ test node]
      ["/atlasdb-timelock-server/var/log/atlasdb-timelock-server-startup.log"])))

(defn read-operation [_ _] {:type :invoke, :f :read-operation, :value nil})

(defn create-client
  "Creates an object that implements the client/Client protocol.
   The object defines how you create a timestamp client, and how to request
   timestamps from it. The first call to this function will return an invalid
   object: you should call 'setup' on the returned object to get a valid one.
  "
  [timestamp-client]
  (reify client/Client
    (setup!
      [this test node]
      "Factory that returns an object implementing client/Client"
        (create-client (TimestampClient/randomizeHostsAndCreate '("n1" "n2" "n3" "n4" "n5"))))

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

(def checker
  (reify checker/Checker
    (check [this test model history opts]
      (.checkClojureHistory (JepsenHistoryChecker/createWithStandardCheckers) history))))

(defn atlasdb-test
  []
  (assoc tests/noop-test
    :os debian/os
    :client (create-client nil)
    :nemesis (nemesis/partition-random-halves)
    :generator (->> read-operation
                    (gen/stagger 0.1)
                    (gen/nemesis
                    (gen/seq (cycle [(gen/sleep 5)
                                     {:type :info, :f :start}
                                     (gen/sleep 20)
                                     {:type :info, :f :stop}])))
                    (gen/time-limit 300))
    :db (create-server)
    :checker checker))
