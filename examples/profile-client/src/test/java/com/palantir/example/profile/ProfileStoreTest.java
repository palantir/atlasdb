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
package com.palantir.example.profile;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.factory.InMemoryLockAndTimestampServiceFactory;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;
import com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile;
import com.palantir.example.profile.schema.ProfileSchema;
import com.palantir.example.profile.schema.generated.ProfileTableFactory;
import com.palantir.example.profile.schema.generated.UserPhotosStreamValueTable;
import com.palantir.timelock.paxos.InMemoryTimeLockRule;
import com.palantir.util.crypto.Sha256Hash;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ProfileStoreTest {
    private static final byte[] IMAGE = new byte[] {0, 1, 2, 3};
    private static final UserProfile USER =
            UserProfile.newBuilder().setBirthEpochDay(0).setName("first last").build();

    @Rule
    public InMemoryTimeLockRule inMemoryTimeLockRule = new InMemoryTimeLockRule();

    private TransactionManager txnMgr;

    @Before
    public void before() {
        txnMgr = TransactionManagers.createInMemory(
                ProfileSchema.INSTANCE.getLatestSchema(),
                new InMemoryLockAndTimestampServiceFactory(inMemoryTimeLockRule.get()));
    }

    @After
    public void after() {
        txnMgr.close();
    }

    @Test
    public void testStore() {
        final UUID userId = storeUser();
        runWithRetry(store -> {
            UserProfile storedData = store.getUserData(userId);
            assertThat(storedData).isEqualTo(USER);
            return userId;
        });
    }

    @Test
    public void testStoreGetDataThrowsAfterTransactionManagerIsClosedThrows() {
        txnMgr.close();
        assertThatThrownBy(this::testStore)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    @Test
    public void testStoreImage() {
        final UUID userId = storeUser();
        storeImage(userId);
        runWithRetry(store -> {
            try (InputStream image = store.getImageForUser(userId)) {
                Sha256Hash hash = Sha256Hash.createFrom(image);
                assertThat(hash).isEqualTo(Sha256Hash.computeHash(IMAGE));
            } catch (IOException e) {
                throw Throwables.throwUncheckedException(e);
            }
            return null;
        });
    }

    @Test
    public void testGetImageBehaviourWhenNoImage() {
        final UUID userId = UUID.randomUUID();
        runWithRetry(store -> {
            InputStream image = store.getImageForUser(userId);
            assertThat(image).isNull();
            return null;
        });
    }

    @Test
    public void testStoreImageThrowsAfterTransactionManagerIsClosedThrows() {
        txnMgr.close();
        assertThatThrownBy(this::testStoreImage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    private void storeImage(final UUID userId) {
        runWithRetry(store -> {
            Sha256Hash imageHash = Sha256Hash.computeHash(IMAGE);
            store.updateImage(userId, imageHash, new ByteArrayInputStream(IMAGE));
            UserProfile storedData = store.getUserData(userId);
            assertThat(storedData).isEqualTo(USER);
            return null;
        });
    }

    @Test
    public void testDeleteImage() {
        final UUID userId = storeUser();
        storeImage(userId);
        runWithRetry(Transaction.TransactionType.AGGRESSIVE_HARD_DELETE, store -> {
            store.deleteImage(userId);
            return userId;
        });
        txnMgr.runTaskWithRetry((TransactionTask<Void, RuntimeException>) txn -> {
            ProfileTableFactory tables = ProfileTableFactory.of();
            UserPhotosStreamValueTable streams = tables.getUserPhotosStreamValueTable(txn);
            assertThat(streams.getAllRowsUnordered().isEmpty()).isTrue();
            return null;
        });
    }

    @Test
    public void testDeleteImageThrowsAfterTransactionManagerIsClosedThrows() {
        txnMgr.close();
        assertThatThrownBy(this::testDeleteImage)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    @Test
    public void testBirthdayIndex() {
        final UUID userId = storeUser();
        runWithRetry(store -> {
            Set<UUID> usersWithBirthday = store.getUsersWithBirthday(USER.getBirthEpochDay());
            assertThat(usersWithBirthday).containsExactlyInAnyOrder(userId);
            return userId;
        });
    }

    @Test
    public void testBirthdayIndexThrowsAfterTransactionManagerIsClosedThrows() {
        txnMgr.close();
        assertThatThrownBy(this::testBirthdayIndex)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Operations cannot be performed on closed TransactionManager.");
    }

    private UUID storeUser() {
        return runWithRetry(store -> {
            UUID userId = store.storeNewUser(USER);
            UserProfile storedData = store.getUserData(userId);
            assertThat(storedData).isEqualTo(USER);
            return userId;
        });
    }

    private <T> T runWithRetry(Function<ProfileStore, T> task) {
        return runWithRetry(Transaction.TransactionType.DEFAULT, task);
    }

    private <T> T runWithRetry(Transaction.TransactionType type, Function<ProfileStore, T> task) {
        return txnMgr.runTaskWithRetry(txn -> {
            txn.setTransactionType(type);
            ProfileStore store = new ProfileStore(txnMgr, txn);
            return task.apply(store);
        });
    }
}
