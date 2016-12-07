/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.hazelcast;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.hazelcast.core.MapStore;
import com.palantir.atlasdb.timelock.hazelcast.generated.tables.Timestamps;

public class HazelcastTimestampMapStore implements MapStore<String, Long> {
    private final Connection conn;

    public HazelcastTimestampMapStore() {
        try {
            Class.forName("org.sqlite.JDBC");
            this.conn = DriverManager.getConnection("jdbc:sqlite:var/data/timestamps.sqlite");
        } catch (ClassNotFoundException | SQLException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public synchronized void store(String client, Long timestamp) {
        DSLContext ctx = DSL.using(conn, SQLDialect.SQLITE);
        int updates = ctx.update(Timestamps.TIMESTAMPS)
                .set(Timestamps.TIMESTAMPS.TIMESTAMP, timestamp.intValue())
                .where(Timestamps.TIMESTAMPS.CLIENT.eq(client))
                .execute();
        if (updates != 1) {
            updates += ctx.insertInto(Timestamps.TIMESTAMPS, Timestamps.TIMESTAMPS.CLIENT, Timestamps.TIMESTAMPS.TIMESTAMP)
            .values(client, timestamp.intValue())
            .onDuplicateKeyUpdate()
            .set(Timestamps.TIMESTAMPS.TIMESTAMP, timestamp.intValue())
            .execute();
        }
        Preconditions.checkState(updates == 1, "Failed to store the timestamp! Tried to write %s", timestamp);
    }

    @Override
    public synchronized void storeAll(Map<String, Long> clientToTimestamp) {
        clientToTimestamp.forEach(this::store);
    }

    @Override
    public synchronized void delete(String client) {
        DSL.using(conn, SQLDialect.SQLITE)
                .deleteFrom(Timestamps.TIMESTAMPS)
                .where(Timestamps.TIMESTAMPS.CLIENT.eq(client))
                .execute();
    }

    @Override
    public synchronized void deleteAll(Collection<String> clients) {
        clients.forEach(this::delete);
    }

    @Override
    public synchronized Long load(String client) {
        return DSL.using(conn, SQLDialect.SQLITE)
                .select()
                .from(Timestamps.TIMESTAMPS)
                .where(Timestamps.TIMESTAMPS.CLIENT.eq(client))
                .fetch(Timestamps.TIMESTAMPS.TIMESTAMP)
                .stream()
                .findFirst()
                .map(Long::new)
                .orElse(0L);
    }

    @Override
    public synchronized Map<String, Long> loadAll(Collection<String> clients) {
        return clients.stream()
                .collect(Collectors.toMap(client -> client, this::load));
    }

    @Override
    public synchronized Iterable<String> loadAllKeys() {
        return DSL.using(conn, SQLDialect.SQLITE)
                .select()
                .from(Timestamps.TIMESTAMPS)
                .fetch(Timestamps.TIMESTAMPS.CLIENT)
                .stream()
                .map(client -> (String) client)
                .collect(Collectors.toList());
    }
}
