/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.logsafe.Arg;

public class CqlQuery {
    final String queryFormat;
    final Arg<?>[] queryArgs;

    CqlQuery(String queryFormat, Arg<?>... args) {
        this.queryFormat = queryFormat;
        this.queryArgs = args;
    }

    List<CellWithTimestamp> executeAndGetCells(
            CqlExecutorImpl.QueryExecutor queryExecutor,
            byte[] rowHintForHostSelection,
            Function<CqlRow, CellWithTimestamp> cellTsExtractor) {
        CqlResult cqlResult = queryExecutor.execute(this, rowHintForHostSelection);
        return CqlQueryUtils.getCells(cellTsExtractor, cqlResult);
    }

    String fullQuery() {
        return String.format(queryFormat, (Object[]) queryArgs);
    }

    @Override
    public String toString() {
        // toString needs to return a safe for logging string, as we use its lazy evaluation to log tracing on
        // CassandraClientImpl#execute_cql3_query.

        String queryString = String.format("query %s", queryFormat);
        String argsString = Arrays.stream(queryArgs)
                .filter(Arg::isSafeForLogging)
                .map(arg -> String.format("%s %s", arg.getName(), arg.getValue()))
                .collect(Collectors.joining(", "));

        return queryString + ", " + argsString;
    }
}
