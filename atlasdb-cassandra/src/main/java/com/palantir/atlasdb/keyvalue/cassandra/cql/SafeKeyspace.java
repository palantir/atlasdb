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

package com.palantir.atlasdb.keyvalue.cassandra.cql;

import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.palantir.logsafe.SafeArg;

@Value.Immutable
public abstract class SafeKeyspace {
    private static final Predicate<String> VALID_KEYSPACE = Pattern.compile("^[0-9a-zA-Z_]+$").asPredicate();

    @Value.Parameter
    public abstract String keyspace();

    @Value.Derived
    public String getQuoted() {
        return '"' + keyspace() + '"';
    }

    public static SafeKeyspace of(String keyspace) {
        if (keyspace.startsWith("\"") && keyspace.endsWith("\"")) {
            return ImmutableSafeKeyspace.of(keyspace.substring(1, keyspace.length() - 1));
        } else {
            return ImmutableSafeKeyspace.of(keyspace);
        }
    }

    @Value.Check
    void checkKeyspaceSafety() {
        Preconditions.checkArgument(VALID_KEYSPACE.test(keyspace()),
                "Received an invalid keyspace, keyspaces must only contain alphanumeric characters or underscores",
                SafeArg.of("keyspaceName", keyspace()));
    }
}