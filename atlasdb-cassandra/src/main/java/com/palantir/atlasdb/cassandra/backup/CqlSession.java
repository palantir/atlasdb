/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.cassandra.backup;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class CqlSession implements Closeable {
    private static final SafeLogger log = SafeLoggerFactory.get(CqlSession.class);

    private static final Duration RETRY_DURATION = Duration.ofMinutes(1L);
    private static final int RETRY_COUNT = 5;

    private final Cluster cluster;
    private final Namespace namespace;
    private final Retryer<TableMetadata> tableMetadataRetryer;

    private Session session;

    private CqlSession(Cluster cluster, Session session, Namespace namespace, Duration retryDuration) {
        this.cluster = cluster;
        this.session = session;
        this.namespace = namespace;

        this.tableMetadataRetryer = new Retryer<>(
                StopStrategies.stopAfterAttempt(RETRY_COUNT),
                WaitStrategies.fixedWait(retryDuration.toMillis(), TimeUnit.MILLISECONDS),
                attempt -> attempt == null || !attempt.hasResult());
    }

    public static CqlSession create(Cluster cluster, Namespace namespace) {
        return CqlSession.create(cluster, namespace, RETRY_DURATION);
    }

    @VisibleForTesting
    static CqlSession create(Cluster cluster, Namespace namespace, Duration retryDuration) {
        Retryer<CqlSession> creationRetryer = new Retryer<>(
                StopStrategies.stopAfterAttempt(RETRY_COUNT),
                WaitStrategies.fixedWait(retryDuration.toMillis(), TimeUnit.MILLISECONDS),
                attempt -> attempt == null || !attempt.hasResult());
        try {
            return creationRetryer.call(() -> createSessionEnsuringMetadataExists(cluster, namespace, retryDuration));
        } catch (ExecutionException e) {
            throw new SafeIllegalStateException(
                    "Failed to execute CqlSession connect", e, SafeArg.of("namespace", namespace));
        } catch (RetryException e) {
            throw new SafeIllegalStateException(
                    "Failed to execute CqlSession connect even with retry", e, SafeArg.of("namespace", namespace));
        }
    }

    private static CqlSession createSessionEnsuringMetadataExists(
            Cluster cluster, Namespace namespace, Duration retryDuration) {
        CqlSession cqlSession = new CqlSession(cluster, cluster.connect(), namespace, retryDuration);
        Optional<KeyspaceMetadata> keyspaceMetadata = cqlSession.getMetadata().getKeyspaceMetadata(namespace);
        if (keyspaceMetadata.isEmpty()) {
            throw new SafeIllegalStateException(
                    "Metadata not found for keyspace while attempting to connect", SafeArg.of("namespace", namespace));
        }
        return cqlSession;
    }

    @Override
    public void close() {
        session.close();
    }

    public CqlMetadata getMetadata() {
        return new CqlMetadata(session.getCluster().getMetadata());
    }

    public TableMetadata getTableMetadata(String tableName) {
        Optional<TableMetadata> maybeTableMetadata = maybeGetTableMetadata(tableName);
        if (maybeTableMetadata.isPresent()) {
            return maybeTableMetadata.get();
        }

        log.info(
                "Couldn't find table metadata; we will refresh and retry",
                SafeArg.of("namespace", namespace),
                SafeArg.of("tableName", tableName));
        return getTableMetadataWithRetry(tableName);
    }

    private TableMetadata getTableMetadataWithRetry(String tableName) {
        try {
            return tableMetadataRetryer.call(() -> {
                refreshSession();
                return maybeGetTableMetadata(tableName).orElseThrow();
            });
        } catch (ExecutionException e) {
            throw new SafeIllegalStateException(
                    "Failed to get table metadata",
                    e,
                    SafeArg.of("namespace", namespace),
                    SafeArg.of("tableName", tableName));
        } catch (RetryException e) {
            throw new SafeIllegalStateException(
                    "Failed to execute CqlSession connect even with retry",
                    e,
                    SafeArg.of("namespace", namespace),
                    SafeArg.of("tableName", tableName));
        }
    }

    private void refreshSession() {
        session.closeAsync();
        session = cluster.connect();
    }

    private Optional<TableMetadata> maybeGetTableMetadata(String tableName) {
        Optional<KeyspaceMetadata> keyspaceMetadata = getMetadata().getKeyspaceMetadata(namespace);
        if (keyspaceMetadata.isEmpty()) {
            return Optional.empty();
        }

        return Optional.ofNullable(keyspaceMetadata.get().getTable(tableName));
    }

    public Set<LightweightOppToken> retrieveRowKeysAtConsistencyAll(List<Statement> selectStatements) {
        return selectStatements.stream()
                .map(statement -> statement.setConsistencyLevel(ConsistencyLevel.ALL))
                .flatMap(select -> StreamSupport.stream(session.execute(select).spliterator(), false))
                .map(row -> row.getToken(CassandraConstants.ROW))
                .map(LightweightOppToken::serialize)
                .collect(Collectors.toSet());
    }

    public PreparedStatement prepare(Statement statement) {
        return session.prepare(statement.toString());
    }

    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }
}
