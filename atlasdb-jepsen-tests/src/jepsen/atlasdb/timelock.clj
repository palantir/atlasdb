(ns jepsen.atlasdb.timelock
  (:require [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.nemesis :as nemesis]
            [jepsen.os.debian :as debian]))

(defn start!
  "Starts timelock"
  [node]
  (c/su
    (info node "Starting timelock server")
    (c/exec :env "JAVA_HOME=/usr/lib/jvm/java-8-oracle" "/timelock-server/service/bin/init.sh" "start")))

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
        (c/upload "resources/atlasdb/timelock-server.tgz" "/")
        (c/exec :mkdir "/timelock-server")
        (c/exec :tar :xf "/timelock-server.tgz" "-C" "/timelock-server" "--strip-components" "1")
        (c/upload "resources/atlasdb/timelock.yml" "/timelock-server/var/conf")
        (c/exec :sed :-i (format "s/<HOSTNAME>/%s/" (name node)) "/timelock-server/var/conf/timelock.yml"))
      (start! node))

    (teardown! [_ _ node]
      (c/su
        (info node "Forcibly killing all Java processes")
        (try (c/exec :pkill "-9" "java") (catch Exception _))
        (info node "Removing any timelock server files")
        (try (c/exec :rm :-rf "/timelock-server") (catch Exception _))
        (try (c/exec :rm :-f "/timelock-server.tgz") (catch Exception _))))

    db/LogFiles
    (log-files [_ test node]
      ["/timelock-server/var/log/timelock-server-startup.log"])))

(defn mostly-small-nonempty-subset-at-most-two
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
    mostly-small-nonempty-subset-at-most-two
    (fn start [test node] (c/su
                            (c/exec :killall :-9 :java))
      ; the following line would also wipe the paxos directory, but we wanted a less aggressive nemesis
      ; which only crashes the node instead. Leaving it commented out in case we decode to reintroduce it
      ;                            (c/exec :rm :-r "/atlasdb-timelock-server/var/data/paxos"))
      [:killed node])
    (fn stop  [test node] (start! node) [:restarted node])))
