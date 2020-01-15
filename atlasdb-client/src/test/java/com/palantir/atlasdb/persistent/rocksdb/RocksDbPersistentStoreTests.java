/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.persistent.rocksdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.persistent.api.ImmutableStoreNamespace;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.atlasdb.persistent.api.PersistentStore.StoreNamespace;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public final class RocksDbPersistentStoreTests {
    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String DEFAULT = "default";
    private static final StoreNamespace NON_EXISTING_NAMESPACE = ImmutableStoreNamespace.builder()
            .humanReadableName("bla")
            .uniqueName(UUID.randomUUID())
            .build();
    private static final byte[] KEY = "key".getBytes();
    private static final byte[] VALUE = "value".getBytes();
    private static final byte[] KEY2 = "key2".getBytes();
    private static final byte[] VALUE2 = "value2".getBytes();

    private PersistentStore timestampMappingStore;
    private StoreNamespace defaultNamespace;

    @Before
    public void before() throws Exception {
        File databaseFolder = temporaryFolder.newFolder();
        RocksDB rocksDb = RocksDB.open(databaseFolder.getAbsolutePath());

        timestampMappingStore = new RocksDbPersistentStore(rocksDb, databaseFolder);
        defaultNamespace = timestampMappingStore.createNamespace(DEFAULT);
    }

    @After
    public void after() throws Exception {
        timestampMappingStore.close();
    }

    @Test
    public void entryMissing() {
        assertThat(timestampMappingStore.get(defaultNamespace, KEY)).isNull();
    }

    @Test
    public void correctlyStored() {
        timestampMappingStore.put(defaultNamespace, KEY, VALUE);
        assertThat(timestampMappingStore.get(defaultNamespace, KEY)).isEqualTo(VALUE);
    }

    @Test
    public void storeNamespaceUniqueness() {
        StoreNamespace differentDefault = timestampMappingStore.createNamespace(DEFAULT);
        assertThat(differentDefault).isNotEqualTo(defaultNamespace);

        timestampMappingStore.put(defaultNamespace, KEY, VALUE);
        assertThat(timestampMappingStore.get(differentDefault, KEY)).isNull();
        timestampMappingStore.dropNamespace(differentDefault);
    }

    @Test
    public void droppingNonExistingFails() {
        assertThatThrownBy(() -> timestampMappingStore.dropNamespace(NON_EXISTING_NAMESPACE))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void getOnNonExistingFails() {
        assertThatThrownBy(() -> timestampMappingStore.get(NON_EXISTING_NAMESPACE, KEY))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void putOnNonExistingFails() {
        assertThatThrownBy(() -> timestampMappingStore.put(NON_EXISTING_NAMESPACE, KEY, VALUE))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void droppingTwoTimesFailsOnSecond() {
        StoreNamespace testNamespace = timestampMappingStore.createNamespace("test");

        timestampMappingStore.dropNamespace(testNamespace);
        assertThatThrownBy(() -> timestampMappingStore.dropNamespace(testNamespace))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void testMultiPut() {
        timestampMappingStore.multiPut(
                defaultNamespace,
                ImmutableMap.of(KEY, VALUE, KEY2, VALUE2));

        assertThat(timestampMappingStore.get(defaultNamespace, KEY)).isEqualTo(VALUE);
        assertThat(timestampMappingStore.get(defaultNamespace, KEY2)).isEqualTo(VALUE2);
    }

    @Test
    public void testMultiGet() {
        timestampMappingStore.put(defaultNamespace, KEY, VALUE);
        timestampMappingStore.put(defaultNamespace, KEY2, VALUE2);

        assertThat(timestampMappingStore.multiGet(defaultNamespace, ImmutableList.of(KEY, KEY2, "random".getBytes())))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        KEY, VALUE,
                        KEY2, VALUE2)
                );
    }
}
