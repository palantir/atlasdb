/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CQLStatementCache {
    Session session, longRunningQuerySession;

    CQLStatementCache(Session session, Session longRunningQuerySession) {
        this.session = session;
        this.longRunningQuerySession = longRunningQuerySession;
    }

    final LoadingCache<String, PreparedStatement> NORMAL_QUERY = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, PreparedStatement>() {
                @Override
                public PreparedStatement load(String query) {
                    return session.prepare(query);
                }
            });


    final LoadingCache<String, PreparedStatement> LONG_RUNNING_QUERY = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, PreparedStatement>() {
                @Override
                public PreparedStatement load(String query) {
                    return longRunningQuerySession.prepare(query);
                }
            });
}
