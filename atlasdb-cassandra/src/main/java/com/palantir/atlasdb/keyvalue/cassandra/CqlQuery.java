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
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class CqlQuery {
    final String queryFormat;
    final Arg<?>[] queryArgs;

    public CqlQuery(String queryFormat, Arg<?>... args) {
        this.queryFormat = queryFormat;
        this.queryArgs = args;
    }

    @Override
    public String toString() {
        return String.format(queryFormat, (Object[]) queryArgs);
    }

    public void logSlowResult(KvsProfilingLogger.LoggingFunction log, Stopwatch timer) {
        Object[] allArgs = new Object[queryArgs.length + 3];
        allArgs[0] = SafeArg.of("queryFormat", queryFormat);
        allArgs[1] = UnsafeArg.of("fullQuery", toString());
        allArgs[2] = LoggingArgs.durationMillis(timer);
        System.arraycopy(queryArgs, 0, allArgs, 3, queryArgs.length);

        log.log("A CQL query was slow: queryFormat = [{}], fullQuery = [{}], durationMillis = {}", allArgs);
    }

    /**
     * Returns an object whose {@link #toString()} implementation returns a log-safe string, and does all formatting
     * lazily. This is intended to be used for trace logging, where the {@link #toString()} method may never be called.
     */
    public Object getLazySafeLoggableObject() {
        return new Object() {
            @Override
            public String toString() {
                String argsString = Arrays.stream(queryArgs)
                        .filter(Arg::isSafeForLogging)
                        .map(arg -> String.format("%s = %s", arg.getName(), arg.getValue()))
                        .collect(Collectors.joining(", "));

                return queryFormat + ": " + argsString;
            }
        };
    }
}
