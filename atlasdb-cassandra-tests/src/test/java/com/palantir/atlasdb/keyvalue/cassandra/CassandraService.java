/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CassandraService {
    private static final Logger log = LoggerFactory.getLogger(CassandraService.class);
    private static final String CASSANDRA_CONFIG = "./cassandra.yaml";
    private static final String CASSANDRA_AUTH_CONFIG = "./cassandra-auth.yaml";

    private static CassandraDaemon daemon;

    private CassandraService() {
        // do not instantiate
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            CassandraService.start(false);
        } catch (IOException e) {
            log.error("failed to start cassandra", e);
        }
    }

    public static synchronized void start(boolean useAuth) throws IOException, InterruptedException {
        File tempDir;
        try {
            tempDir = Files.createTempDirectory("cassandra").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Cannot create data directory.");
        }
        tempDir.deleteOnExit();
        String cassandraYaml = useAuth ? CASSANDRA_AUTH_CONFIG : CASSANDRA_CONFIG;
        System.setProperty("cassandra.config", new File(cassandraYaml).toURI().toString());
        System.setProperty("cassandra.storagedir", tempDir.getPath());

        daemon = new CassandraDaemon(true);
        daemon.init(null);
        daemon.start();
        Thread.sleep(5000); //give cassandra the chance to start up properly
    }

    public static synchronized void stop() {
        if (daemon == null) {
            // server never started, do nothing
            return;
        }
        daemon.deactivate();
        daemon = null;
    }
}
