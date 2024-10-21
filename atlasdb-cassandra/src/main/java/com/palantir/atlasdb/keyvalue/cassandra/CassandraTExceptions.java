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

import com.google.errorprone.annotations.CompileTimeConstant;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.Arg;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;

public final class CassandraTExceptions {
    private CassandraTExceptions() {}

    public static AtlasDbDependencyException mapToUncheckedException(
            @CompileTimeConstant final String logMessage, Throwable throwable, Arg<?>... args) {
        if (throwable instanceof TimedOutException) {
            return new CassandraTimedOutException(throwable, args);
        }
        if (throwable instanceof UnavailableException) {
            return new InsufficientConsistencyException(logMessage, throwable);
        }
        if (throwable instanceof InsufficientConsistencyException) {
            return new InsufficientConsistencyException(logMessage, throwable);
        }
        return new AtlasDbDependencyException(throwable);
    }

    public static AtlasDbDependencyException mapToUncheckedException(Throwable throwable, Arg<?>... args) {
        if (throwable instanceof TimedOutException) {
            return new CassandraTimedOutException(throwable, args);
        }
        if (throwable instanceof UnavailableException) {
            return new InsufficientConsistencyException(throwable, args);
        }
        return new AtlasDbDependencyException(throwable, args);
    }
}
