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

package com.palantir.atlasdb.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.TableRefAndUuid;

public class TableRefAndUuidTest {
    @Test
    public void persistAndHydrateWithNamespace() {
        TableRefAndUuid tableRefAndUuid = TableRefAndUuid.of(
                TableReference.createFromFullyQualifiedName("test.test"),
                UUID.randomUUID());

        assertThat(TableRefAndUuid.BYTES_HYDRATOR.hydrateFromBytes(tableRefAndUuid.persistToBytes()))
                .isEqualTo(tableRefAndUuid);
    }

    @Test
    public void persistAndHydrateWithoutNamespace() {
        TableRefAndUuid tableRefAndUuid = TableRefAndUuid.of(
                TableReference.createWithEmptyNamespace("test"),
                UUID.randomUUID());

        assertThat(TableRefAndUuid.BYTES_HYDRATOR.hydrateFromBytes(tableRefAndUuid.persistToBytes()))
                .isEqualTo(tableRefAndUuid);
    }
}
