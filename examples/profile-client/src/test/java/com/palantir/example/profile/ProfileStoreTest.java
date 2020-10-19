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

import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;
import com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile;
import com.palantir.example.profile.schema.ProfileSchema;
import com.palantir.example.profile.schema.generated.ProfileTableFactory;
import com.palantir.example.profile.schema.generated.UserPhotosStreamValueTable;
import com.palantir.util.crypto.Sha256Hash;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ProfileStoreTest {
    private static final byte[] IMAGE = new byte[] {0, 1, 2, 3};
    private static final UserProfile USER = UserProfile.newBuilder()
            .setBirthEpochDay(0)
            .setName("first last")
            .build();

    private final TransactionManager txnMgr =
            TransactionManagers.createInMemory(ProfileSchema.INSTANCE.getLatestSchema());

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @After
    public void after() throws Exception {
        txnMgr.close();
    }

    @Test
    public void testStore() {
        final UUID userId = storeUser();
        runWithRetry(store -> {
            UserProfile storedData = store.getUserData(userId);
            Assert.assertEquals(USER, storedData);
            return userId;
        });
    }

    @Test
    public void testStoreGetDataThrowsAfterTransactionManagerIsClosedThrows() throws Exception {
        txnMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager.");
        testStore();
    }

    @Test
    public void testStoreImage() {
        final UUID userId = storeUser();
        storeImage(userId);
        runWithRetry(store -> {
            InputStream image = store.getImageForUser(userId);
            try {
                Sha256Hash hash = Sha256Hash.createFrom(image);
                Assert.assertEquals(Sha256Hash.computeHash(IMAGE), hash);
            } catch (IOException e) {
                throw Throwables.throwUncheckedException(e);
            } finally {
                Closeables.closeQuietly(image);
            }
            return null;
        });
    }

    @Test
    public void testGetImageBehaviourWhenNoImage() {
        final UUID userId = UUID.randomUUID();
        runWithRetry(store -> {
            InputStream image = store.getImageForUser(userId);
            assertThat(image, nullValue());
            return null;
        });
    }

    @Test
    public void testStoreImageThrowsAfterTransactionManagerIsClosedThrows() throws Exception {
        txnMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager.");
        testStoreImage();
    }

    private void storeImage(final UUID userId) {
        runWithRetry(store -> {
            Sha256Hash imageHash = Sha256Hash.computeHash(IMAGE);
            store.updateImage(userId, imageHash, new ByteArrayInputStream(IMAGE));
            UserProfile storedData = store.getUserData(userId);
            Assert.assertEquals(USER, storedData);
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
            Assert.assertTrue(streams.getAllRowsUnordered().isEmpty());
            return null;
        });
    }

    @Test
    public void testDeleteImageThrowsAfterTransactionManagerIsClosedThrows() throws Exception {
        txnMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager.");
        testDeleteImage();
    }

    @Test
    public void testBirthdayIndex() {
        final UUID userId = storeUser();
        runWithRetry(store -> {
            Set<UUID> usersWithBirthday = store.getUsersWithBirthday(USER.getBirthEpochDay());
            Assert.assertEquals(ImmutableSet.of(userId), usersWithBirthday);
            return userId;
        });
    }

    @Test
    public void testBirthdayIndexThrowsAfterTransactionManagerIsClosedThrows() throws Exception {
        txnMgr.close();
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Operations cannot be performed on closed TransactionManager.");
        testDeleteImage();
    }

    private UUID storeUser() {
        return runWithRetry(store -> {
            UUID userId = store.storeNewUser(USER);
            UserProfile storedData = store.getUserData(userId);
            Assert.assertEquals(USER, storedData);
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
