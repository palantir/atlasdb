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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraConstants;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CqlSession implements Closeable {
    private final Session session;

    public CqlSession(Session session) {
        this.session = session;
    }

    @Override
    public void close() {
        session.close();
    }

    public CqlMetadata getMetadata() {
        return new CqlMetadata(session.getCluster().getMetadata());
    }

    public Set<LightweightOppToken> executeAtConsistencyAll(List<Statement> selectStatements) {
        return selectStatements.stream()
                .map(statement -> statement.setConsistencyLevel(ConsistencyLevel.ALL))
                .flatMap(select -> StreamSupport.stream(session.execute(select).spliterator(), false))
                .map(row -> row.getToken(CassandraConstants.ROW))
                .map(LightweightOppToken::serialize)
                .collect(Collectors.toSet());
    }
}
