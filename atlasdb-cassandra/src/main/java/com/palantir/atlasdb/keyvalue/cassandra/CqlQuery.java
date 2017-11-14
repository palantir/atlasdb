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

import java.util.List;
import java.util.function.Function;

import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.keyvalue.cassandra.sweep.CellWithTimestamp;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class CqlQuery {
    private final String queryFormat;
    private final Arg<?>[] queryArgs;

    public CqlQuery(String queryFormat, Arg<?>... args) {
        this.queryFormat = queryFormat;
        this.queryArgs = args;
    }

    public List<CellWithTimestamp> executeAndGetCells(
            CqlExecutorImpl.QueryExecutor queryExecutor,
            byte[] rowHintForHostSelection,
            Function<CqlRow, CellWithTimestamp> cellTsExtractor) {
        CqlResult cqlResult = KvsProfilingLogger.maybeLog(
                () -> queryExecutor.execute(rowHintForHostSelection, toString()),
                this::logSlowResult,
                this::logResultSize);
        return CqlQueryUtils.getCells(cellTsExtractor, cqlResult);
    }

    private void logSlowResult(KvsProfilingLogger.LoggingFunction log, Stopwatch timer) {
        Object[] allArgs = new Object[queryArgs.length + 3];
        allArgs[0] = SafeArg.of("queryFormat", queryFormat);
        allArgs[1] = UnsafeArg.of("fullQuery", toString());
        allArgs[2] = LoggingArgs.durationMillis(timer);
        System.arraycopy(queryArgs, 0, allArgs, 3, queryArgs.length);

        log.log("A CQL query was slow: queryFormat = [{}], fullQuery = [{}], durationMillis = {}", allArgs);
    }

    private void logResultSize(KvsProfilingLogger.LoggingFunction log, CqlResult result) {
        log.log("and returned {} rows",
                SafeArg.of("numRows", result.getRows().size()));
    }

    @Override
    public String toString() {
        return String.format(queryFormat, (Object[]) queryArgs);
    }
}
