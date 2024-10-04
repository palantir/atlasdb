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

import com.palantir.logsafe.Arg;
import com.palantir.logsafe.Safe;
import com.palantir.logsafe.SafeLoggable;
import com.palantir.logsafe.exceptions.SafeExceptions;
import java.util.List;
import javax.annotation.Nullable;

public class CassandraTimedOutException extends RuntimeException implements SafeLoggable {
    private static final long serialVersionUID = 1L;
    private static final String LOG_MESSAGE =
            "Cassandra query threw a TimedOut exception. Possible reasons and actions to resolve include:\n"
                + "1. Reason: AtlasDB clients are requesting too much data from Cassandra.\n"
                + "   Resolution: Change the query to request less data.\n"
                + "2. Reason: Data that has been deleted is being read in the query (e.g. A large amount of"
                + " tombstones).\n"
                + "   Resolution: Run a compaction on your Cassandra server.\n"
                + "3. Reason: Cassandra is struggling, possibly due to another large query, server health issues, or a"
                + " network outage.\n"
                + "   Resolution: Ask your CassandraOps to check the state of the Cassandra server.";
    private final List<Arg<?>> args;

    public CassandraTimedOutException(Throwable throwable, Arg<?>... args) {
        this(throwable, List.of(args));
    }

    private CassandraTimedOutException(@Nullable Throwable cause, List<Arg<?>> args) {
        super(SafeExceptions.renderMessage(LOG_MESSAGE, args.toArray(new Arg[0])), cause);
        this.args = args;
    }

    @Override
    public @Safe String getLogMessage() {
        return LOG_MESSAGE;
    }

    @Override
    public List<Arg<?>> getArgs() {
        return args;
    }
}
