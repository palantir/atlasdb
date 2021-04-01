/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.store;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.paxos.SqliteConnections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqliteBlobStoreTest {
    private static final byte[] BYTES_1 = PtBytes.toBytes("tom");
    private static final byte[] BYTES_2 = PtBytes.toBytes("cat");

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private SqliteBlobStore blobStore;

    @Before
    public void setup() {
        blobStore = SqliteBlobStore.create(SqliteConnections.getPooledDataSource(tempFolder.getRoot().toPath()));
    }

    @Test
    public void initiallyEmpty() {
        assertThat(blobStore.getValue(BlobStoreUseCase.PERSISTENCE_STORAGE)).isEmpty();
    }

    @Test
    public void retrievesStoredValues() {
        blobStore.putValue(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
        assertBlobStoreContains(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
    }

    @Test
    public void overwritesStoredValues() {
        blobStore.putValue(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
        assertBlobStoreContains(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
        blobStore.putValue(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_2);
        assertBlobStoreContains(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_2);
        blobStore.putValue(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
        assertBlobStoreContains(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
    }

    @Test
    public void maintainsSeparateBlobsForSeparateUseCases() {
        blobStore.putValue(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
        assertThat(blobStore.getValue(BlobStoreUseCase.INTERNAL_TESTING_RESERVED)).isEmpty();

        blobStore.putValue(BlobStoreUseCase.INTERNAL_TESTING_RESERVED, BYTES_2);
        assertBlobStoreContains(BlobStoreUseCase.PERSISTENCE_STORAGE, BYTES_1);
        assertBlobStoreContains(BlobStoreUseCase.INTERNAL_TESTING_RESERVED, BYTES_2);
    }

    private void assertBlobStoreContains(BlobStoreUseCase useCase, byte[] expected) {
        assertThat(blobStore.getValue(useCase)).hasValue(expected);
    }
}