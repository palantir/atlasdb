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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.thrift.TException;

public class CassandraTimedOutException extends RuntimeException implements SafeLoggable {
    private static final long serialVersionUID = 1L;
    private final String logMessage;
    private final List<Arg<?>> args;

    public CassandraTimedOutException(Throwable throwable) {
        this(throwable, List.of());
    }

    public CassandraTimedOutException(Throwable throwable, Arg<?>... args) {
        this(throwable, toArgList(args));
    }

    private CassandraTimedOutException(
            @Nullable Throwable cause,
            List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(reasonsForTimedOutException(), args.toArray(new Arg[0])), cause);
        this.logMessage = reasonsForTimedOutException();
        this.args = args;
    }

    @Override
    public @Safe String getLogMessage() {
        return logMessage;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }


    private static String reasonsForTimedOutException() {
      return "Cassandra query threw a TimedOut exception. Possible reasons and possible actions to resolve: " +
              "1. Reason: atlasdb clients are requesting too much data from Cassandra. Resolution: Change query to request less data." +
              "2. Reason: Data deleted is being read in the query (eg// Large amount of tombstones). Resolution: Run a compaction on your cassandra server." +
              "3. Reason: Cassandra is struggling, either due to another large query or server health or network outage. Resolution: Ask your CassandraOps to check the state of the Cassandra server."
    }

    private static List<Arg<?>> toArgList(Arg<?>[] args) {
        return ImmutableList.<Arg<?>>builderWithExpectedSize(args.length + 1)
                .add(args)
                .build();
    }
}