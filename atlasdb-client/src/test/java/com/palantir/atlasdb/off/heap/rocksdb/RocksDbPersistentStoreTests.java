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

package com.palantir.atlasdb.off.heap.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;

import com.palantir.atlasdb.local.storage.api.PersistentStore;
import com.palantir.atlasdb.local.storage.api.PersistentStore.Serializer;
import com.palantir.atlasdb.local.storage.api.PersistentStore.StoreNamespace;
import com.palantir.atlasdb.off.heap.ImmutableStoreNamespace;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public final class RocksDbPersistentStoreTests {
    private static final Serializer<Long, Long> SERIALIZER =
            new Serializer<Long, Long>() {
                @Override
                public byte[] serializeKey(Long key) {
                    return ValueType.VAR_LONG.convertFromJava(key);
                }

                @Override
                public Long deserializeKey(byte[] key) {
                    return (Long) ValueType.VAR_LONG.convertToJava(key, 0);
                }

                @Override
                public byte[] serializeValue(Long key, Long value) {
                    return ValueType.VAR_LONG.convertFromJava(value - key);
                }

                @Override
                public Long deserializeValue(Long key, byte[] value) {
                    return key + (Long) ValueType.VAR_LONG.convertToJava(value, 0);
                }
            };

    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String DEFAULT = "default";
    private static final StoreNamespace NON_EXISTING_NAMESPACE = ImmutableStoreNamespace.builder()
            .humanReadableName("bla")
            .uniqueName(UUID.randomUUID())
            .build();

    private PersistentStore<Long, Long> timestampMappingStore;
    private StoreNamespace defaultNamespace;

    @Before
    public void before() throws Exception {
        RocksDB rocksDb = RocksDB.open(temporaryFolder.newFolder().getAbsolutePath());

        timestampMappingStore = new RocksDbPersistentStore<>(rocksDb, SERIALIZER);
        defaultNamespace = timestampMappingStore.createNamespace(DEFAULT);
    }

    @After
    public void after() throws Exception {
        timestampMappingStore.close();
    }

    @Test
    public void entryMissing() {
        assertThat(timestampMappingStore.get(defaultNamespace, 1L)).isNull();
    }

    @Test
    public void correctlyStored() {
        timestampMappingStore.put(defaultNamespace, 1L, 3L);
        assertThat(timestampMappingStore.get(defaultNamespace, 1L)).isEqualTo(3L);
    }

    @Test
    public void storeNamespaceUniqueness() {
        StoreNamespace differentDefault = timestampMappingStore.createNamespace(DEFAULT);
        assertThat(differentDefault).isNotEqualTo(defaultNamespace);

        timestampMappingStore.put(defaultNamespace, 1L, 3L);
        assertThat(timestampMappingStore.get(differentDefault, 1L)).isNull();
        timestampMappingStore.dropNamespace(differentDefault);
    }

    @Test
    public void droppingNonExitingFails() {
        assertThatThrownBy(() -> timestampMappingStore.dropNamespace(NON_EXISTING_NAMESPACE))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void getOnNonExistingFails() {
        assertThatThrownBy(() -> timestampMappingStore.get(NON_EXISTING_NAMESPACE, 1L))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void putOnNonExistingFails() {
        assertThatThrownBy(() -> timestampMappingStore.put(NON_EXISTING_NAMESPACE, 1L, 2L))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void droppingTwoTimesFailsOnSecond() {
        StoreNamespace testNamespace = timestampMappingStore.createNamespace("test");

        timestampMappingStore.dropNamespace(testNamespace);
        assertThatThrownBy(() -> timestampMappingStore.dropNamespace(testNamespace))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }
}
