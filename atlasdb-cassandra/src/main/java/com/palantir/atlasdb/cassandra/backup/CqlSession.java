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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import java.io.Closeable;

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

    public PreparedStatement prepare(Statement statement) {
        return session.prepare(statement.toString());
    }

    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }
}
