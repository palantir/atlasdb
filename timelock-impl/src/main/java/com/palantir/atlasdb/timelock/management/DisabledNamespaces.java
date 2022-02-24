/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.SingleNodeUpdateResponse;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlBatch;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.transaction.Transaction;

public class DisabledNamespaces {
    private static final SafeLogger log = SafeLoggerFactory.get(DisabledNamespaces.class);

    private final Jdbi jdbi;

    private Set<Namespace> disabledNamespaces;

    private DisabledNamespaces(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static DisabledNamespaces create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        DisabledNamespaces disabledNamespaces = new DisabledNamespaces(jdbi);
        disabledNamespaces.initialize();
        return disabledNamespaces;
    }

    private void initialize() {
        execute(Queries::createTable);
        disabledNamespaces = disabledNamespaces();
    }

    public boolean isDisabled(Namespace namespace) {
        return disabledNamespaces.contains(namespace);
    }

    public boolean isEnabled(Namespace namespace) {
        return !isDisabled(namespace);
    }

    @VisibleForTesting
    Set<Namespace> disabledNamespaces() {
        return execute(Queries::getAllStates).stream().map(Namespace::of).collect(Collectors.toSet());
    }

    public Map<Namespace, String> getNamespacesLockedWithDifferentLockId(
            Set<Namespace> namespaces, String expectedLockId) {
        return execute(dao -> dao.getNamespacesWithLockConflict(namespaces, expectedLockId));
    }

    public SingleNodeUpdateResponse disable(DisableNamespacesRequest request) {
        Set<Namespace> namespaces = request.getNamespaces();
        String lockId = request.getLockId();
        SingleNodeUpdateResponse response = execute(dao -> dao.disableAll(namespaces, lockId));
        if (response.isSuccessful()) {
            disabledNamespaces.addAll(namespaces);
            log.info("Successfully disabled namespaces", SafeArg.of("namespaces", namespaces));
        } else {
            log.error(
                    "Failed to disable namespaces, as some were already disabled",
                    SafeArg.of("namespaces", namespaces),
                    SafeArg.of("lockId", lockId),
                    SafeArg.of("lockedNamespace", response.lockedNamespaces()));
        }
        return response;
    }

    public SingleNodeUpdateResponse reEnable(ReenableNamespacesRequest request) {
        String lockId = request.getLockId();
        SingleNodeUpdateResponse response = execute(dao -> dao.reEnableAll(request.getNamespaces(), lockId));
        if (response.isSuccessful()) {
            disabledNamespaces.removeAll(request.getNamespaces());
        } else {
            Map<Namespace, String> conflictingNamespaces = response.lockedNamespaces();
            Set<Namespace> namespacesWithExpectedLock =
                    Sets.difference(request.getNamespaces(), conflictingNamespaces.keySet());
            disabledNamespaces.removeAll(namespacesWithExpectedLock);
            log.error(
                    "Failed to re-enable all namespaces, as some were disabled with a different lock ID.",
                    SafeArg.of("reEnabledNamespaces", namespacesWithExpectedLock),
                    SafeArg.of("expectedLockId", lockId),
                    SafeArg.of("conflictingNamespaces", conflictingNamespaces));
        }
        return response;
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS disabled (namespace TEXT PRIMARY KEY, lockId UUID)")
        boolean createTable();

        @Transaction
        default Map<Namespace, String> getNamespacesWithLockConflict(Set<Namespace> namespaces, String expectedLockId) {
            return getLockedNamespaces(namespaces)
                    .filter(lockIdForNamespace -> !lockIdForNamespace.equals(expectedLockId))
                    .collectToMap();
        }

        @Transaction
        default SingleNodeUpdateResponse disableAll(Set<Namespace> namespaces, String lockId) {
            Map<Namespace, String> conflictingNamespaces = getNamespacesWithLockConflict(namespaces, lockId);

            if (!conflictingNamespaces.isEmpty()) {
                return SingleNodeUpdateResponse.failed(conflictingNamespaces);
            }

            Set<String> namespaceNames = namespaces.stream().map(Namespace::get).collect(Collectors.toSet());
            disable(namespaceNames, lockId);
            return SingleNodeUpdateResponse.successful();
        }

        @Transaction
        default SingleNodeUpdateResponse reEnableAll(Set<Namespace> namespaces, String lockId) {
            Map<Namespace, String> namespacesWithLockConflict = getNamespacesWithLockConflict(namespaces, lockId);

            if (!namespacesWithLockConflict.isEmpty()) {
                // Unlock the namespaces we can
                Set<Namespace> namespacesWithExpectedLock =
                        Sets.difference(namespaces, namespacesWithLockConflict.keySet());
                unlockNamespaces(namespacesWithExpectedLock);

                return SingleNodeUpdateResponse.failed(namespacesWithLockConflict);
            }

            unlockNamespaces(namespaces);
            return SingleNodeUpdateResponse.successful();
        }

        private void unlockNamespaces(Set<Namespace> namespaces) {
            namespaces.stream().map(Namespace::get).forEach(this::delete);
        }

        private KeyedStream<Namespace, String> getLockedNamespaces(Set<Namespace> namespaces) {
            return KeyedStream.of(namespaces)
                    .map(Namespace::get)
                    .map(this::getLockId)
                    .flatMap(Optional::stream);
        }

        // Can ignore conflicts, provided we called getNamespacesWithLockConflict prior to insertion.
        @SqlBatch("INSERT INTO disabled (namespace, lockId) VALUES (?, ?) ON CONFLICT DO NOTHING")
        void disable(@Bind("namespace") Set<String> namespaces, @Bind("lockId") String lockId);

        @SqlQuery("SELECT lockId FROM disabled WHERE namespace = ?")
        Optional<String> getLockId(String namespace);

        @SqlQuery("SELECT namespace FROM disabled WHERE namespace = ?")
        Optional<String> getState(String namespace);

        @SqlQuery("SELECT namespace FROM disabled")
        Set<String> getAllStates();

        @SqlUpdate("DELETE FROM disabled WHERE namespace = ?")
        boolean delete(String namespace);
    }
}
