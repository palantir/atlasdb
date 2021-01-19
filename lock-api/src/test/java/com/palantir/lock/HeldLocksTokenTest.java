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
package com.palantir.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedMap;
import java.math.BigInteger;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public final class HeldLocksTokenTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSerializationDeserialization() throws Exception {
        SortedMap<LockDescriptor, LockMode> lockMap =
                ImmutableSortedMap.of(StringLockDescriptor.of("foo"), LockMode.READ);
        HeldLocksToken heldLocksToken = new HeldLocksToken(
                BigInteger.valueOf(0),
                LockClient.of("foo"),
                0,
                0,
                LockCollections.of(lockMap),
                SimpleTimeDuration.of(1, TimeUnit.SECONDS),
                0L,
                "Dummy Thread");

        HeldLocksToken deserializedHeldLocksToken =
                mapper.readValue(mapper.writeValueAsString(heldLocksToken), HeldLocksToken.class);
        assertThat(deserializedHeldLocksToken).isEqualTo(heldLocksToken);
    }
}
