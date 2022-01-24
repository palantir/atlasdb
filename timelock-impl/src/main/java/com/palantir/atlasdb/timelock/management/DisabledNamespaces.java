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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
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

    public DisableNamespacesResponse disable(DisableNamespacesRequest request) {
        return execute(dao -> dao.disableAll(request.getNamespaces(), request.getLockId()));
    }

    public ReenableNamespacesResponse reEnable(ReenableNamespacesRequest request) {
        return execute(dao -> dao.reEnableAll(request.getNamespaces(), request.getLockId()));
    }

    private <T> T execute(Function<Queries, T> call) {
        return jdbi.withExtension(Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS disabled (namespace TEXT PRIMARY KEY, lockId UUID)")
        boolean createTable();

        @Transaction
        default DisableNamespacesResponse disableAll(Set<Namespace> namespaces, UUID lockId) {
            Set<Namespace> alreadyDisabled = namespaces.stream()
                    .filter(ns -> getState(ns.toString()).isPresent())
                    .collect(Collectors.toSet());
            if (!alreadyDisabled.isEmpty()) {
                log.error(
                        "Failed to disable namespaces, as some were already disabled",
                        SafeArg.of("namespaces", namespaces),
                        SafeArg.of("alreadyDisabledNamespaces", alreadyDisabled));
                return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                        // we're querying a single node and are consistent with ourselves
                        .consistentlyDisabledNamespaces(alreadyDisabled)
                        .build());
            }

            Set<String> namespaceNames = namespaces.stream().map(Namespace::get).collect(Collectors.toSet());
            disable(namespaceNames, lockId);
            log.info("Successfully disabled namespaces", SafeArg.of("namespaces", namespaces));
            return DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(lockId));
        }

        @Transaction
        default ReenableNamespacesResponse reEnableAll(Set<Namespace> namespaces, UUID lockId) {
            Set<Namespace> namespacesWithLockConflict = namespaces.stream()
                    .filter(ns -> !getLockId(ns.get()).orElse(lockId).equals(lockId))
                    .collect(Collectors.toSet());
            if (!namespacesWithLockConflict.isEmpty()) {
                log.error(
                        "Failed to re-ensable namespaces, as some were disabled with a different lock ID",
                        SafeArg.of("namespaces", namespaces),
                        SafeArg.of("failedNamespaces", namespacesWithLockConflict));
                return ReenableNamespacesResponse.of(false, namespacesWithLockConflict);
            }

            namespaces.stream().map(Namespace::get).forEach(this::delete);
            return ReenableNamespacesResponse.of(true, ImmutableSet.of());
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
