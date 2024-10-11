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

import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.Arg;
import java.util.Optional;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

public final class CassandraTExceptions {
    private static final String UNAVAILABLE_EXCEPTION_LOG_MESSAGE=
                "Cassandra key value service threw a InsufficientConsistencyException." +
                "Thrown when an operation could not be performed because the required consistency could not be met."
    private CassandraTExceptions() {}

    public static RuntimeException mapToUncheckedException(Optional<String> maybeLogMessage, Throwable throwable, Arg<?>... args) {
        if (throwable instanceof TimedOutException) {
            return new CassandraTimedOutException(throwable, args);
        }
        if (throwable instanceof UnavailableException) {
            String logMessage = maybeLogMessage.orElse(UNAVAILABLE_EXCEPTION_LOG_MESSAGE);
            return new InsufficientConsistencyException(logMessage, throwable);
        }
        return Throwables.throwUncheckedException(throwable);
    }
}
