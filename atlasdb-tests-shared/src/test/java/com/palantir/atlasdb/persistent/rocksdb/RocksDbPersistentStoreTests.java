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
import com.google.protobuf.ByteString;
import com.palantir.atlasdb.persistent.api.PersistentStore;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.RocksDB;

public final class RocksDbPersistentStoreTests {
    @ClassRule
    public static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final PersistentStore.Handle NON_EXISTING_NAMESPACE = PersistentStore.Handle.newHandle();
    private static final ByteString KEY = ByteString.copyFromUtf8("key");
    private static final ByteString VALUE = ByteString.copyFromUtf8("value");
    private static final ByteString KEY2 = ByteString.copyFromUtf8("key2");
    private static final ByteString VALUE2 = ByteString.copyFromUtf8("value2");

    private PersistentStore persistentStore;
    private PersistentStore.Handle defaultNamespace;

    @Before
    public void before() throws Exception {
        File databaseFolder = temporaryFolder.newFolder();
        RocksDB rocksDb = RocksDB.open(databaseFolder.getAbsolutePath());

        persistentStore = new RocksDbPersistentStore(rocksDb, databaseFolder);
        defaultNamespace = persistentStore.createSpace();
    }

    @After
    public void after() throws Exception {
        persistentStore.close();
    }

    @Test
    public void entryMissing() {
        assertThat(persistentStore.get(defaultNamespace, KEY)).isEmpty();
    }

    @Test
    public void correctlyStored() {
        persistentStore.put(defaultNamespace, KEY, VALUE);
        assertThat(persistentStore.get(defaultNamespace, KEY)).hasValue(VALUE);
    }

    @Test
    public void storeNamespaceUniqueness() {
        PersistentStore.Handle differentDefault = persistentStore.createSpace();
        assertThat(differentDefault).isNotEqualTo(defaultNamespace);

        persistentStore.put(defaultNamespace, KEY, VALUE);
        assertThat(persistentStore.get(differentDefault, KEY)).isEmpty();
        persistentStore.dropStoreSpace(differentDefault);
    }

    @Test
    public void droppingNonExistingFails() {
        assertThatThrownBy(() -> persistentStore.dropStoreSpace(NON_EXISTING_NAMESPACE))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void getOnNonExistingFails() {
        assertThatThrownBy(() -> persistentStore.get(NON_EXISTING_NAMESPACE, KEY))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void putOnNonExistingFails() {
        assertThatThrownBy(() -> persistentStore.put(NON_EXISTING_NAMESPACE, KEY, VALUE))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void droppingTwoTimesFailsOnSecond() {
        PersistentStore.Handle testNamespace = persistentStore.createSpace();

        persistentStore.dropStoreSpace(testNamespace);
        assertThatThrownBy(() -> persistentStore.dropStoreSpace(testNamespace))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void testMultiPut() {
        persistentStore.put(defaultNamespace, ImmutableMap.of(KEY, VALUE, KEY2, VALUE2));

        assertThat(persistentStore.get(defaultNamespace, KEY)).hasValue(VALUE);
        assertThat(persistentStore.get(defaultNamespace, KEY2)).hasValue(VALUE2);
    }

    @Test
    public void testMultiGet() {
        persistentStore.put(defaultNamespace, KEY, VALUE);
        persistentStore.put(defaultNamespace, KEY2, VALUE2);

        assertThat(persistentStore.get(defaultNamespace, ImmutableList.of(KEY, KEY2, ByteString.copyFromUtf8("bla"))))
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        KEY, VALUE,
                        KEY2, VALUE2));
    }
}
