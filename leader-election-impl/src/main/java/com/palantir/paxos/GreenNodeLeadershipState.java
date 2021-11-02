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

package com.palantir.paxos;

import com.palantir.sls.versions.OrderableSlsVersion;
import java.util.Optional;
import java.util.function.Function;
import javax.sql.DataSource;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.immutables.JdbiImmutables;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

public final class GreenNodeLeadershipState {
    private static final int NODE_ID = 0;

    private final Jdbi jdbi;

    private GreenNodeLeadershipState(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public static GreenNodeLeadershipState create(DataSource dataSource) {
        Jdbi jdbi = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin());
        jdbi.getConfig(JdbiImmutables.class).registerImmutable(Client.class);
        GreenNodeLeadershipState state = new GreenNodeLeadershipState(jdbi);
        state.initialise();
        return state;
    }

    private void initialise() {
        execute(Queries::createGreenNodeLeadershipStateTable);
    }

    public Optional<Long> getLatestAttemptTime(OrderableSlsVersion currentVersion) {
        return execute(dao -> dao.getLatestAttemptTime(NODE_ID, currentVersion.getValue()));
    }

    public boolean setLatestAttemptTime(OrderableSlsVersion currentVersion, Long attemptTimeMillis) {
        return execute(dao -> dao.updateLatestAttemptTime(NODE_ID, currentVersion.getValue(), attemptTimeMillis));
    }

    private <T> T execute(Function<GreenNodeLeadershipState.Queries, T> call) {
        return jdbi.withExtension(GreenNodeLeadershipState.Queries.class, call::apply);
    }

    public interface Queries {
        @SqlUpdate("CREATE TABLE IF NOT EXISTS greenNodeLeadershipState (nodeId INTEGER, productVersion TEXT, "
                + "attemptTime BIGINT, PRIMARY KEY(nodeId))")
        boolean createGreenNodeLeadershipStateTable();

        @SqlUpdate("INSERT OR REPLACE INTO greenNodeLeadershipState (node, productVersion, attemptTime) VALUES "
                + "(:nodeId, :productVersion, :attemptTime)")
        boolean updateLatestAttemptTime(
                @Bind("nodeId") int nodeId,
                @Bind("productVersion") String productVersion,
                @Bind("attemptTime") long attemptTime);

        @SqlQuery("SELECT attemptTime FROM greenNodeLeadershipState "
                + "WHERE nodeId = :nodeId AND productVersion = :productVersion")
        Optional<Long> getLatestAttemptTime(@Bind("nodeId") int nodeId, @Bind("productVersion") String productVersion);
    }
}
