(ns jepsen.atlasdb.timelock
  (:require [clojure.tools.logging :refer :all]
            [jepsen.control :as c]
            [jepsen.db :as db]
            [jepsen.os.debian :as debian])
  (:import com.palantir.atlasdb.http.TimelockUtils))

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
        (c/exec :sed :-i (format "s/<HOSTNAME>/%s/" (name node)) "/atlasdb-timelock-server/var/conf/timelock.yml")
        (info node "Starting timelock server")
        (c/exec :env "JAVA_HOME=/usr/lib/jvm/java-8-oracle" "/atlasdb-timelock-server/service/bin/init.sh" "start")
        (info node "Waiting until timelock node is ready")
        (TimelockUtils/waitUntilHostReady (name node))))

    (teardown! [_ _ node]
      (c/su
        (try (c/exec "/atlasdb-timelock-server/service/bin/init.sh" "stop") (catch Exception _))
        (try (c/exec :rm :-rf "/atlasdb-timelock-server") (catch Exception _))
        (try (c/exec :rm :-f "/atlasdb-timelock-server.tgz") (catch Exception _))))

    db/LogFiles
    (log-files [_ test node]
      ["/atlasdb-timelock-server/var/log/atlasdb-timelock-server-startup.log"])))
