/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import org.apache.thrift.TException;

public class CassandraTimedOutException extends TException {

    public CassandraTimedOutException(Throwable throwable) {
        super(reasonsForTimedOutException(), throwable);
    }

    private static String reasonsForTimedOutException() {
      return "Cassandra query threw a TimedOut exception. Possible reasons and possible actions to resolve: " +
              "1. Reason: atlasdb clients are requesting too much data from Cassandra. Resolution: Change query to request less data." +
              "2. Reason: Data deleted is being read in the query (eg// Large amount of tombstones). Resolution: Run a compaction on your cassandra server." +
              "3. Reason: Cassandra is struggling, either due to another large query or server health or network outage. Resolution: Ask your CassandraOps to check the state of the Cassandra server."
    }
}