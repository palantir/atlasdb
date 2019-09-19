/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.processors.AutoDelegate;

@AutoDelegate
public interface CqlClient extends AutoCloseable {

    interface CqlQuery<R> {
        ListenableFuture<R> execute();
    }

    interface CqlQueryBuilder<R> {

        CqlQueryBuilder<R> setQuerySpec(CqlQuerySpec<R> cqlQuerySpec);

        CqlQueryBuilder<R> setArg(String argumentName, Object argument);

        <T> CqlQueryBuilder<T> setRowStreamAccumulator(RowStreamAccumulator<T> rowStreamAccumulator);

        CqlQuery<R> build();
    }

    <R> CqlQueryBuilder<R> asyncQueryBuilder();
}
