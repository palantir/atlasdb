/**
 * Copyright 2015 Palantir Technologies
 * <p>
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://opensource.org/licenses/BSD-3-Clause
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableList;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.core.ConditionTimeoutException;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;

/**
 * Utilities for ETE tests
 * Created by aloro on 12/04/2016.
 */
class CassandraTestTools {
    private CassandraTestTools() {
        // Empty constructor for utility class
    }

    static void waitTillServiceIsUp(String host, int port, Duration timeout) {
        try {
            Awaitility.await()
                    .pollInterval(50, TimeUnit.MILLISECONDS)
                    .atMost(timeout.getMillis(), TimeUnit.MILLISECONDS)
                    .until(isPortListening(host, port));
        } catch (ConditionTimeoutException e) {
            throw new IllegalStateException("Timeout for port " + port + " on host " + host + ".");
        }
    }

    private static Callable<Boolean> isPortListening(String host, int port) {
        return () -> {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                return true;
            } catch (IOException e) {
                return false;
            }
        };
    }

    static Future async(ExecutorService executorService, Runnable callable) {
        return executorService.submit(callable);
    }

    static void assertThatFutureDidNotSucceedYet(Future future) throws InterruptedException {
        if (future.isDone()) {
            try {
                future.get();
                throw new AssertionError("Future task should have failed but finished successfully");
            } catch (ExecutionException e) {
                // if execution is done, we expect it to have failed
            }
        }
    }

    static void dropKeyspaceIfExists(final CassandraKeyValueServiceConfig config) throws TException {
        try {
            CassandraClientPool clientPool = new CassandraClientPool(config);
            String keyspace = config.keyspace();
            clientPool.run(client -> {
                client.system_drop_keyspace(keyspace);
                return null;
            });
        } catch (InvalidRequestException | CassandraClientFactory.ClientCreationFailedException e) {
            // This happens when the keyspace didn't exist in the first place. In this case, our work is done.
            System.out.println("Didn't drop keyspace " + config.keyspace() + ", likely because it didn't exist");
            System.out.println("Message: " + e.getMessage());
            System.out.println("Cause: " + e.getCause().getMessage());
        }
    }

    static void createKeyspace(ImmutableCassandraKeyValueServiceConfig config) throws TException {
        // Shamelessly copied from CassandraVerifier. TODO does it make sense to deduplicate this code?
        InetSocketAddress host = config.servers().iterator().next();
        Cassandra.Client client = CassandraClientFactory.getClientInternal(host, config.credentials(),
                config.ssl(), config.socketTimeoutMillis(), config.socketQueryTimeoutMillis());
        KsDef ks = new KsDef(config.keyspace(), CassandraConstants.NETWORK_STRATEGY, ImmutableList.<CfDef>of());
        CassandraVerifier.checkAndSetReplicationFactor(client, ks, true, config.replicationFactor(), config.safetyDisabled());
        ks.setDurable_writes(true);
        System.out.println("Creating keyspace: " + config.keyspace());
        client.system_add_keyspace(ks);
        CassandraKeyValueServices.waitForSchemaVersions(client, "(adding the initial empty keyspace)", config.schemaMutationTimeoutMillis());
    }
}
