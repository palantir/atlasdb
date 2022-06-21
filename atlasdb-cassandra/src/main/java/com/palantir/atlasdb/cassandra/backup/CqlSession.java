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
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
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
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CqlSession implements Closeable {
    private static final Duration RETRY_DURATION = Duration.ofMinutes(1L);
    private static final int RETRY_COUNT = 5;

    private final Session session;

    public CqlSession(Session session) {
        this.session = session;
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
            return creationRetryer.call(() -> new CqlSession(cluster.connect()));
        } catch (ExecutionException e) {
            throw new SafeIllegalStateException(
                    "Failed to execute CqlSession connect", e, SafeArg.of("namespace", namespace));
        } catch (RetryException e) {
            throw new SafeIllegalStateException(
                    "Failed to execute CqlSession connect even with retry", e, SafeArg.of("namespace", namespace));
        }
    }

    //    private static CqlSession createSessionEnsuringMetadataExists(Cluster cluster, Namespace namespace) {
    //        CqlSession cqlSession = new CqlSession(cluster.connect());
    //        KeyspaceMetadata keyspaceMetadata = cqlSession.getMetadata().getKeyspaceMetadata(namespace);
    //        if (keyspaceMetadata == null) {
    //            throw new SafeIllegalStateException("Metadata not found for keyspace. We'll retry the connection");
    //        }
    //        return cqlSession;
    //    }

    @Override
    public void close() {
        session.close();
    }

    public CqlMetadata getMetadata() {
        return new CqlMetadata(session.getCluster().getMetadata());
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
