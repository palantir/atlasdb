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
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CQLStatementCache {
    final Session session;
    final Session longRunningQuerySession;

    CQLStatementCache(Session session, Session longRunningQuerySession) {
        this.session = session;
        this.longRunningQuerySession = longRunningQuerySession;
    }

    final LoadingCache<String, PreparedStatement> normalQuery = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, PreparedStatement>() {
                @Override
                public PreparedStatement load(String query) {
                    return prepareSessionWithErrorHandling(session, query);
                }
            });


    final LoadingCache<String, PreparedStatement> longRunningQuery = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, PreparedStatement>() {
                @Override
                public PreparedStatement load(String query) {
                    return prepareSessionWithErrorHandling(longRunningQuerySession, query);
                }
            });

    private PreparedStatement prepareSessionWithErrorHandling(Session currentSession, String query) {
        try {
            return currentSession.prepare(query);
        } catch (NoHostAvailableException e) {
            if (currentSession.isClosed()) {
                currentSession = currentSession.getCluster().newSession();
                return currentSession.prepare(query);
            } else {
                throw e;
            }
        }
    }
}
