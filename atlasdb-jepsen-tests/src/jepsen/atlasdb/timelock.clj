(ns jepsen.atlasdb.timelock
  (:require [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.nemesis :as nemesis]
            [jepsen.os.debian :as debian])
  (:import com.palantir.atlasdb.http.TimelockUtils))

(defn start!
  "Starts timelock"
  [node]
  (c/su
    (info node "Starting timelock server")
    (c/exec :env "JAVA_HOME=/usr/lib/jvm/java-8-oracle" "/atlasdb-timelock-server/service/bin/init.sh" "start")
    (info node "Waiting until timelock node is ready")
    (TimelockUtils/waitUntilHostReady (name node))
    (info node "timelock server ready")))

(defn create-db
  "Creates an object that implements the db/DB protocol.
   This object defines how to setup and teardown a timelock server on a given node, and specifies where the log files
   can be found.
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
        (c/exec :sed :-i (format "s/<HOSTNAME>/%s/" (name node)) "/atlasdb-timelock-server/var/conf/timelock.yml"))
      (start! node))

    (teardown! [_ _ node]
      (c/su
        (info node "Forcibly killing all Java processes")
        (try (c/exec :pkill "-9" "java") (catch Exception _))
        (info node "Removing any timelock server files")
        (try (c/exec :rm :-rf "/atlasdb-timelock-server") (catch Exception _))
        (try (c/exec :rm :-f "/atlasdb-timelock-server.tgz") (catch Exception _))))

    db/LogFiles
    (log-files [_ test node]
      ["/atlasdb-timelock-server/var/log/atlasdb-timelock-server-startup.log"])))

(defn mostly-small-nonempty-subset-at-most-three
  "Returns a subset of the given collection, with a logarithmically decreasing
  probability of selecting more elements. Always selects at least one element.
      (->> #(mostly-small-nonempty-subset [1 2 3 4 5])
           repeatedly
           (map count)
           (take 10000)
           frequencies
           sort)
      ; => ([1 3824] [2 2340] [3 1595] [4 1266] [5 975])"
  [xs]
  (-> xs
      count
      inc
      Math/log
      rand
      Math/exp
      long
      (take (shuffle xs))))

(def crash-nemesis
  "A nemesis that crashes a random subset of nodes."
  (nemesis/node-start-stopper
    mostly-small-nonempty-subset-at-most-three
    (fn start [test node] (c/su
                            (c/exec :killall :-9 :java)
                            (c/exec :rm :-r "/atlasdb-timelock-server/var/data/paxos"))
                          [:killed node])
    (fn stop  [test node] (start! node) [:restarted node])))
