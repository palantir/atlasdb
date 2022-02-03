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
import java.util.UUID;
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

    public DisabledNamespaces(Jdbi jdbi) {
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
    }

    public boolean isDisabled(Namespace namespace) {
        return execute(dao -> dao.getState(namespace.get())).isPresent();
    }

    public boolean isEnabled(Namespace namespace) {
        return execute(dao -> dao.getState(namespace.get())).isEmpty();
    }

    public Set<Namespace> disabledNamespaces() {
        return execute(Queries::getAllStates).stream().map(Namespace::of).collect(Collectors.toSet());
    }

    public Map<Namespace, UUID> getNamespacesLockedWithDifferentLockId(Set<Namespace> namespaces, UUID expectedLockId) {
        return execute(dao -> dao.getIncorrectlyLockedNamespaces(namespaces, expectedLockId));
    }

    public SingleNodeUpdateResponse disable(DisableNamespacesRequest request) {
        return execute(dao -> dao.disableAll(request.getNamespaces(), request.getLockId()));
    }

    public SingleNodeUpdateResponse reEnable(ReenableNamespacesRequest request) {
        return execute(dao -> dao.reEnableAll(request.getNamespaces(), request.getLockId()));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS disabled (namespace TEXT PRIMARY KEY, lockId UUID)")
        boolean createTable();

        @Transaction
        default Map<Namespace, UUID> getIncorrectlyLockedNamespaces(Set<Namespace> namespaces, UUID expectedLockId) {
            return getLockedNamespaces(namespaces)
                    .filter(lockId -> !lockId.equals(expectedLockId))
                    .collectToMap();
        }

        @Transaction
        default SingleNodeUpdateResponse disableAll(Set<Namespace> namespaces, UUID lockId) {
            Map<Namespace, UUID> lockedNamespaces =
                    getLockedNamespaces(namespaces).collectToMap();

            if (!lockedNamespaces.isEmpty()) {
                log.error(
                        "Failed to disable namespaces, as some were already disabled",
                        SafeArg.of("namespaces", namespaces),
                        SafeArg.of("lockId", lockId),
                        SafeArg.of("lockedNamespace", lockedNamespaces));
                return SingleNodeUpdateResponse.of(false, lockedNamespaces);
            }

            Set<String> namespaceNames = namespaces.stream().map(Namespace::get).collect(Collectors.toSet());
            disable(namespaceNames, lockId);
            log.info("Successfully disabled namespaces", SafeArg.of("namespaces", namespaces));
            return SingleNodeUpdateResponse.successful();
        }

        @Transaction
        default SingleNodeUpdateResponse reEnableAll(Set<Namespace> namespaces, UUID lockId) {
            Map<Namespace, UUID> namespacesWithLockConflict = getLockedNamespaces(namespaces)
                    .filter(lockIdForNamespace -> !lockIdForNamespace.equals(lockId))
                    .collectToMap();

            if (!namespacesWithLockConflict.isEmpty()) {
                // Unlock the namespaces we can
                Set<Namespace> namespacesWithExpectedLock =
                        Sets.difference(namespaces, namespacesWithLockConflict.keySet());
                unlockNamespaces(namespacesWithExpectedLock);

                log.error(
                        "Failed to re-enable all namespaces, as some were disabled with a different lock ID.",
                        SafeArg.of("reEnabledNamespaces", namespacesWithExpectedLock),
                        SafeArg.of("expectedLockId", lockId),
                        SafeArg.of("conflictingNamespaces", namespacesWithLockConflict));
                return SingleNodeUpdateResponse.of(false, namespacesWithLockConflict);
            }

            unlockNamespaces(namespaces);
            return SingleNodeUpdateResponse.successful();
        }

        private void unlockNamespaces(Set<Namespace> namespaces) {
            namespaces.stream().map(Namespace::get).forEach(this::delete);
        }

        private KeyedStream<Namespace, UUID> getLockedNamespaces(Set<Namespace> namespaces) {
            return KeyedStream.of(namespaces)
                    .map(Namespace::get)
                    .map(this::getLockId)
                    .flatMap(Optional::stream);
        }

        @SqlBatch("INSERT INTO disabled (namespace, lockId) VALUES (?, ?)")
        void disable(@Bind("namespace") Set<String> namespaces, @Bind("lockId") UUID lockId);

        @SqlQuery("SELECT lockId FROM disabled WHERE namespace = ?")
        Optional<UUID> getLockId(String namespace);

        @SqlQuery("SELECT namespace FROM disabled WHERE namespace = ?")
        Optional<String> getState(String namespace);

        @SqlQuery("SELECT namespace FROM disabled")
        Set<String> getAllStates();

        @SqlUpdate("DELETE FROM disabled WHERE namespace = ?")
        boolean delete(String namespace);
    }
}
