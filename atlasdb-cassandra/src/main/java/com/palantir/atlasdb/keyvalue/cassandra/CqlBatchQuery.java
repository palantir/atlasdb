/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import org.immutables.value.Value;

import com.google.common.collect.Lists;
import com.palantir.logsafe.Arg;

@Value.Immutable
public abstract class CqlBatchQuery {
    abstract List<CqlQuery> individualQueryStatements();

    public static ImmutableCqlBatchQuery.Builder builder() {
        return ImmutableCqlBatchQuery.builder();
    }

    @Value.Lazy
    public CqlQuery toCqlQuery() {
        StringBuilder queryFormat = new StringBuilder();
        List<Arg<?>> queryArgs = Lists.newArrayList();
        queryFormat.append("BEGIN UNLOGGED BATCH\n"); // Safe, because all updates are on the same partition key
        individualQueryStatements().forEach(query -> {
            queryFormat.append(query.safeQueryFormat());
            queryArgs.addAll(query.args());
        });
        queryFormat.append("APPLY BATCH;");

        return CqlQuery.builder()
                .safeQueryFormat(queryFormat.toString())
                .args(queryArgs)
                .build();
    }
}
