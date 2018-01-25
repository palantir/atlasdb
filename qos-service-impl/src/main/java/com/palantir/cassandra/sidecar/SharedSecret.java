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
package com.palantir.cassandra.sidecar;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Copied from internal sls Cassandra project.
 */
@Value.Immutable
public abstract class SharedSecret {

    @JsonValue
    public abstract String secret();

    @JsonCreator
    public static SharedSecret valueOf(String sharedSecret) {
        return ImmutableSharedSecret.builder().secret(sharedSecret).build();
    }

    @Override
    public final String toString() {
        return secret();
    }

}
