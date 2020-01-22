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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.persistent.api.ImmutableStoreNamespace;
import com.palantir.atlasdb.persistent.api.PhysicalPersistentStore;
import com.palantir.atlasdb.persistent.api.PhysicalPersistentStore.StoreNamespace;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.File;
import java.util.UUID;
import okio.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;

public final class RocksDbPhysicalPersistentStoreTests {
    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final String DEFAULT = "default";
    private static final StoreNamespace NON_EXISTING_NAMESPACE = ImmutableStoreNamespace.builder()
            .humanReadableName("bla")
            .uniqueName(UUID.randomUUID())
            .build();
    private static final ByteString KEY = ByteString.encodeUtf8("key");
    private static final ByteString VALUE = ByteString.encodeUtf8("value");
    private static final ByteString KEY2 = ByteString.encodeUtf8("key2");
    private static final ByteString VALUE2 = ByteString.encodeUtf8("value2");

    private PhysicalPersistentStore timestampMappingStore;
    private StoreNamespace defaultNamespace;

    @Before
    public void before() throws Exception {
        File databaseFolder = temporaryFolder.newFolder();
        RocksDB rocksDb = RocksDB.open(databaseFolder.getAbsolutePath());

        timestampMappingStore = new RocksDbPhysicalPersistentStore(rocksDb, databaseFolder);
        defaultNamespace = timestampMappingStore.createNamespace(DEFAULT);
    }

    @After
    public void after() throws Exception {
        timestampMappingStore.close();
    }

    @Test
    public void entryMissing() {
        assertThat(timestampMappingStore.get(defaultNamespace, KEY)).isEmpty();
    }

    @Test
    public void correctlyStored() {
        timestampMappingStore.put(defaultNamespace, KEY, VALUE);
        assertThat(timestampMappingStore.get(defaultNamespace, KEY)).hasValue(VALUE);
    }

    @Test
    public void storeNamespaceUniqueness() {
        StoreNamespace differentDefault = timestampMappingStore.createNamespace(DEFAULT);
        assertThat(differentDefault).isNotEqualTo(defaultNamespace);

        timestampMappingStore.put(defaultNamespace, KEY, VALUE);
        assertThat(timestampMappingStore.get(differentDefault, KEY)).isEmpty();
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
        timestampMappingStore.put(
                defaultNamespace,
                ImmutableMap.of(KEY, VALUE, KEY2, VALUE2));

        assertThat(timestampMappingStore.get(defaultNamespace, KEY)).hasValue(VALUE);
        assertThat(timestampMappingStore.get(defaultNamespace, KEY2)).hasValue(VALUE2);
    }

    @Test
    public void testMultiGet() {
        timestampMappingStore.put(defaultNamespace, KEY, VALUE);
        timestampMappingStore.put(defaultNamespace, KEY2, VALUE2);

        assertThat(
                timestampMappingStore.get(defaultNamespace, ImmutableList.of(KEY, KEY2, ByteString.encodeUtf8("bla"))))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        KEY, VALUE,
                        KEY2, VALUE2)
                );
    }
}
