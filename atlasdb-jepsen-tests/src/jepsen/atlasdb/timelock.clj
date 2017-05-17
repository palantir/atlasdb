(ns jepsen.atlasdb.timelock
  (:require [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.os.debian :as debian]))

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
        (c/exec :sed :-i (format "s/<HOSTNAME>/%s/" (name node)) "/timelock-server/var/conf/timelock.yml")
        (info node "Starting timelock server")
        (c/exec :env "JAVA_HOME=/usr/lib/jvm/java-8-oracle" "/timelock-server/service/bin/init.sh" "start")))

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
