/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.base.Stopwatch;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
public abstract class CqlQuery {
    public abstract String safeQueryFormat();

    public abstract List<Arg<?>> args();

    @Override
    public String toString() {
        return String.format(safeQueryFormat(), args().toArray());
    }

    public static ImmutableCqlQuery.Builder builder() {
        return ImmutableCqlQuery.builder();
    }

    public void logSlowResult(KvsProfilingLogger.LoggingFunction log, Stopwatch timer) {
        Object[] allArgs = new Object[args().size() + 3];
        allArgs[0] = SafeArg.of("queryFormat", safeQueryFormat());
        allArgs[1] = UnsafeArg.of("fullQuery", toString());
        allArgs[2] = LoggingArgs.durationMillis(timer);
        System.arraycopy(args().toArray(), 0, allArgs, 3, args().size());

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
                String argsString = args().stream()
                        .filter(Arg::isSafeForLogging)
                        .map(arg -> String.format("%s = %s", arg.getName(), arg.getValue()))
                        .collect(Collectors.joining(", "));

                return safeQueryFormat() + ": " + argsString;
            }
        };
    }
}
