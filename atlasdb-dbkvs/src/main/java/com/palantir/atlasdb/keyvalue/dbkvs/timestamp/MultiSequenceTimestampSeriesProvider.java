/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.pool.ConnectionManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads all of the series from a timestamp table written to by timestamp bound stores using the
 * {@link MultiSequencePhysicalBoundStoreStrategy} strategy.
 */
public final class MultiSequenceTimestampSeriesProvider implements TimestampSeriesProvider {
    private static final Logger log = LoggerFactory.getLogger(MultiSequenceTimestampSeriesProvider.class);

    private final ConnectionManager connManager;
    private final TableReference timestampTable;

    private MultiSequenceTimestampSeriesProvider(ConnectionManager connManager, TableReference timestampTable) {
        this.connManager = connManager;
        this.timestampTable = timestampTable;
    }

    public static TimestampSeriesProvider create(
            KeyValueService rawKvs, TableReference tableReference, boolean initializeAsync) {
        if (initializeAsync) {
            log.warn("Asynchronous initialization not implemented, will initialize synchronously.");
        }

        Preconditions.checkArgument(
                rawKvs instanceof ConnectionManagerAwareDbKvs,
                "DbAtlasDbFactory expects a raw kvs of type ConnectionManagerAwareDbKvs, found %s",
                rawKvs.getClass());
        ConnectionManagerAwareDbKvs dbkvs = (ConnectionManagerAwareDbKvs) rawKvs;
        return new MultiSequenceTimestampSeriesProvider(dbkvs.getConnectionManager(), tableReference);
    }

    @Override
    public Set<TimestampSeries> getKnownSeries() {
        try (Connection connection = connManager.getConnection()) {
            String sql = String.format("SELECT client FROM %s FOR UPDATE", timestampTable.getQualifiedName());
            QueryRunner runner = new QueryRunner();
            return runner.query(connection, sql, rs -> {
                ImmutableSet.Builder<TimestampSeries> series = ImmutableSet.builder();
                while (rs.next()) {
                    series.add(TimestampSeries.of(rs.getString("client")));
                }
                return series.build();
            });
        } catch (SQLException e) {
            throw PalantirSqlException.create(e);
        }
    }
}
