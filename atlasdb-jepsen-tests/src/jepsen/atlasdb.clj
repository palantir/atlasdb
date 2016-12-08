(ns jepsen.atlasdb
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
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
            [jepsen.tests :as tests]))

(def ATLASDB_LOG_FILES ["/atlasdb-ete/var/log/atlasdb-ete.log",
                        "/atlasdb-ete/var/log/atlasdb-ete-request.log",
                        "/atlasdb-ete/var/log/atlasdb-ete-startup.log"])
(def CASSANDRA_LOG_FILES ["/var/log/cassandra/system.log",
                          "/var/log/cassandra/debug.log"])

(def CASSANDRA_VERSION "2.2.6")

(defn install-atlasdb
  "Install AtlasDB ETE Server"
  [node]
  (c/su
   (info node "installing AtlasDB")
   (debian/install-jdk8!)
   (c/upload "resources/atlasdb/atlasdb-ete.tgz" "/")
   (c/exec :rm "-rf" "/atlasdb-ete")
   (c/exec :mkdir "/atlasdb-ete")
   (c/exec :tar "xf" "/atlasdb-ete.tgz" "-C" "/atlasdb-ete" "--strip-components" "1")
   (info node "waiting for Cassandra")
   (c/exec (c/lit "(while [[ $? != 52 ]]; do sleep 1; curl n4:9160; done; true)"))
   (info node "running AtlasDB")
   (c/exec :mkdir "-p" "/atlasdb-ete/var/conf")
   (c/upload "resources/atlasdb/atlasdb-ete.yml.template" "/atlasdb-ete/var/conf/")
   (c/cd
    "/atlasdb-ete"
    (c/exec :sed (format "s/{{LOCAL_HOSTNAME}}/%s/g" (name node)) "var/conf/atlasdb-ete.yml.template" :> "var/conf/atlasdb-ete.yml")
    (c/exec "service/bin/init.sh" "start"))
   (c/exec (c/lit (format "(false; while [[ $? != 0 ]]; do sleep 1; curl -sfo /dev/null http://%s:3828/cas; done)" (name node))))
   (info node "AtlasDB is ready")))

(defn install-cassandra
  [node version]
  (c/su
   (info node "installing Cassandra" version)
   (debian/add-repo!
    "cassandra"
    "deb http://www.apache.org/dist/cassandra/debian 22x main")
   (debian/install {:cassandra version})
   (info node "starting Cassandra")
   (c/upload "resources/cassandra/cassandra.yaml.template" "/etc/cassandra/")
   (c/exec :sed (c/lit "\"s/{{LOCAL_ADDRESS}}/$(hostname -I)/g\"") "/etc/cassandra/cassandra.yaml.template" :> "/etc/cassandra/cassandra.yaml")
   (c/upload "resources/cassandra/cassandra-env.sh" "/etc/cassandra/")
   (c/exec :service :cassandra :start)
   (info node "Cassandra is ready")))

(defn teardown-atlasdb
  [node]
  (c/su
   (info node "tearing down AtlasDB")
   (c/cd
    "/atlasdb-ete"
    (c/exec "service/bin/init.sh" "stop"))))

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
      (if (some #{node} (:nodes test))
        (install-atlasdb node))
      (if (some #{node} (:cassandra-nodes test))
        (install-cassandra node CASSANDRA_VERSION)))

    (teardown! [_ test node]
      (if (some #{node} (:nodes test))
        (teardown-atlasdb node))
      (if (some #{node} (:cassandra-nodes test))
        (teardown-cassandra node)))

    db/LogFiles
    (log-files [_ test node]
      (cond
        (some #{node} (:nodes test)) ATLASDB_LOG_FILES
        (some #{node} (:cassandra-nodes)) CASSANDRA_LOG_FILES))))

(defn atlasdb-get [node]
  (json/parse-string (:body (http/get (format "http://%s:3828/cas" (name node)) {:content-type :json}))))

(defn atlasdb-put! [node new-value]
  (let [contents (json/generate-string new-value)]
    (http/put (format "http://%s:3828/cas" (name node)) {:content-type :json :body contents})))

(defn atlasdb-cas! [node old-value new-value]
  (let [contents (json/generate-string {:oldValue old-value :newValue new-value})]
    (json/parse-string (:body (http/patch (format "http://%s:3828/cas" (name node)) {:content-type :json :body contents})))))

(defrecord CASClient [node]
  client/Client
  (setup! [this test node]
    (atlasdb-put! node nil)
    (assoc this :node node))

  (invoke! [this test op]
    (case (:f op)
      :read  (try (assoc op :type :ok :value (atlasdb-get node))
                  (catch Exception e
                    (warn e "Read failed")
                    (assoc op :type :fail)))

      :write (do (atlasdb-put! node (:value op))
                 (assoc op :type :ok))
      :cas   (let [[value value'] (:value op)
                   ok? (atlasdb-cas! node value value')]
               (assoc op :type (if ok? :ok :fail)))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single consul node."
  []
  (CASClient. nil))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defmacro with-cassandra
  "Wraps body in Cassandra setup and teardown."
  [test & body]
  `(c/with-ssh (:ssh ~test)
    (jepsen/with-resources [sessions#
                            (bound-fn* c/session)
                            c/disconnect
                            (:cassandra-nodes ~test)]
      (let [test# (->> sessions#
                      (map vector (:cassandra-nodes ~test))
                      (into {})
                      (assoc ~test :sessions))]
        (jepsen/with-os test#
          (jepsen/with-db test#
            ~@body))))))

(defn atlasdb-test
  []
  (assoc tests/noop-test
         :name "atlasdb"
         :nodes ["n1" "n2" "n3"]
         :cassandra-nodes ["n4"]
         :os debian/os
         :db (db)
         :client (cas-client)
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
