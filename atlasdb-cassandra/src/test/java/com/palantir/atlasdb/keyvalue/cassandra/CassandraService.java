/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CassandraService {
    private static final String CASSANDRA_CONFIG = "./cassandra.yaml";

    private static CassandraDaemon daemon;

    private CassandraService() {
        // do not instantiate
    }

    public static void main(String[] args) {
        CassandraService.start();
    }

    public static synchronized void start() {
        File tempDir;
        try {
            tempDir = Files.createTempDirectory("cassandra").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Cannot create data directory.");
        }
        tempDir.deleteOnExit();
        System.setProperty("cassandra.config", new File(CASSANDRA_CONFIG).toURI().toString());
        System.setProperty("cassandra.storagedir", tempDir.getPath());

        daemon = new TestCassandraDaemon();
        daemon.activate();
    }

    public static synchronized void stop() {
        if (daemon == null) {
            // server never started, do nothing
            return;
        }
        daemon.deactivate();
        daemon = null;
    }

    private static class TestCassandraDaemon extends CassandraDaemon {
        private static final Logger logger = LoggerFactory.getLogger(CassandraService.TestCassandraDaemon.class);

        // Copied from CassandraDaemon::stop to fix issue on Windows
        @Override
        public void stop() {
            // On linux, this doesn't entirely shut down Cassandra, just the RPC server.
            // jsvc takes care of taking the rest down
            logger.info("Cassandra shutting down...");
            thriftServer.stop();
            nativeServer.stop();

            // Not needed for a test daemon
            // On windows, we need to stop the entire system as prunsrv doesn't have the jsvc hooks
            // We rely on the shutdown hook to drain the node
            // if (FBUtilities.isWindows())
            //    System.exit(0);

            if (jmxServer != null)
            {
                try
                {
                    jmxServer.stop();
                } catch (IOException e)
                {
                    logger.error("Error shutting down local JMX server: ", e);
                }
            }
        }
    }
}
