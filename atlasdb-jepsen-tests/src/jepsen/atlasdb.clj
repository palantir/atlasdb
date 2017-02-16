(ns jepsen.atlasdb
  (:require [cheshire.core :as json]
            [clojure.tools.logging :refer :all]
            [jepsen.core :as jepsen]
            [jepsen.checker :as checker]
            [jepsen.checker.timeline :as timeline]
            [jepsen.client :as client]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.generator :as gen]
            [jepsen.model :as model]
            [jepsen.nemesis :as nemesis]
            [jepsen.os.debian :as debian]
            [jepsen.tests :as tests])
  (:import com.palantir.atlasdb.AtlasDbEteServer))

(def CASSANDRA_LOG_FILES ["/var/log/cassandra/system.log",
                          "/var/log/cassandra/debug.log"])

(def CASSANDRA_VERSION "2.2.8")

(defn install-cassandra
  [node version]
  (c/su
   (info node "installing Cassandra" version)
   (debian/add-repo!
    "cassandra"
    "deb http://www.apache.org/dist/cassandra/debian 22x main"
    "pool.sks-keyservers.net" "0xA278B781FE4B2BDA")
   (debian/install {:cassandra version})
   (info node "starting Cassandra")
   (c/upload "resources/cassandra/cassandra.yaml.template" "/etc/cassandra/")
   (c/exec :sed (c/lit "\"s/{{LOCAL_ADDRESS}}/$(hostname -I)/g\"") "/etc/cassandra/cassandra.yaml.template" :> "/etc/cassandra/cassandra.yaml")
   (c/upload "resources/cassandra/cassandra-env.sh" "/etc/cassandra/")
   (c/exec :service :cassandra :start)
   (info node "Cassandra is ready")))

(defn teardown-cassandra
  [node]
  (c/su
   (info node "tearing down Cassandra")
   (c/exec :service :cassandra :stop)
   (c/exec :rm "-rf" "/var/lib/cassandra/{saved_caches,data,commitlog}")))

(defn db
  "AtlasDB node setup."
  []
  (reify db/DB
    (setup! [_ test node]
        (install-cassandra node CASSANDRA_VERSION))

    (teardown! [_ test node]
        (teardown-cassandra node))

    db/LogFiles
    (log-files [_ test node]
         CASSANDRA_LOG_FILES)))

(defn atlasdb-get [node] "")

(defn atlasdb-put! [node new-value] "")

(defn atlasdb-cas! [node old-value new-value] "")

(defn create-client
  "Creates an object that implements the client/Client protocol.
   The object defines how you create a lock client, and how to request locks from it. The first call to this
   function will return an invalid object: you should call 'setup' on the returned object to get a valid one.
  "
  [node]
  (reify client/Client
    (setup!
      [this test node]
      "Factory that returns an object implementing client/Client"
      (AtlasDbEteServer/main "server" "resources/atlasdb/atlasdb-ete.yml.template")
      (create-client node))

    (invoke! [this test op]
      (case (:f op)
        :read (try (assoc op :type :ok :value (atlasdb-get node))
                (catch Exception e
                  (warn e "Read failed")
                  (assoc op :type :fail)))

        :write (do (atlasdb-put! node (:value op))
                 (assoc op :type :ok))
        :cas (let [[value value'] (:value op)
                   ok? (atlasdb-cas! node value value')]
               (assoc op :type (if ok? :ok :fail)))))

    (teardown! [_ test])))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn atlasdb-test
  []
  (assoc tests/noop-test
         :name "atlasdb"
         :nodes ["n1"]
         :os debian/os
         :db (db)
         :client (create-client nil)
         :nemesis (nemesis/partition-random-halves)
         :model (model/cas-register)
         :checker (checker/compose
                   {:html (timeline/html)
                    :perf (checker/perf)
                    :linear checker/linearizable})
         :generator (->> (gen/mix [r w cas])
                         (gen/stagger 1)
                         (gen/nemesis
                          (gen/seq (cycle [(gen/sleep 5)
                                           {:type :info, :f :start}
                                           (gen/sleep 5)
                                           {:type :info, :f :stop}])))
                         (gen/time-limit 15))))
