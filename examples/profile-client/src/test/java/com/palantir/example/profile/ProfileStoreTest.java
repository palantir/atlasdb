/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.example.profile;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.palantir.atlasdb.memory.InMemoryAtlasDbFactory;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.common.base.Throwables;
import com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile;
import com.palantir.example.profile.schema.ProfileSchema;
import com.palantir.example.profile.schema.generated.ProfileTableFactory;
import com.palantir.example.profile.schema.generated.UserPhotosStreamValueTable;
import com.palantir.util.crypto.Sha256Hash;

public class ProfileStoreTest {
    TransactionManager txnMgr;
    UserProfile user = UserProfile.newBuilder().setBirthEpochDay(0).setName("first last").build();
    public static final byte[] IMAGE = new byte[] {0, 1, 2, 3};

    @Before
    public void setup() {
        txnMgr = InMemoryAtlasDbFactory.createInMemoryTransactionManager(ProfileSchema.INSTANCE);
    }

    interface ProfileStoreTask<T> {
        public T execute(ProfileStore store);
    }

    @Test
    public void testStore() {
        final long userId = storeUser();
        runWithRetry(new ProfileStoreTask<Long>() {
            @Override
            public Long execute(ProfileStore store) {
                UserProfile storedData = store.getUserData(userId);
                Assert.assertEquals(user, storedData);
                return userId;
            }
        });
    }

    @Test
    public void testStoreImage() {
        final long userId = storeUser();
        storeImage(userId);
        runWithRetry(new ProfileStoreTask<Void>() {
            @Override
            public Void execute(ProfileStore store) {
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
            }
        });
    }

    private long storeImage(final long userId) {
        long streamId = runWithRetry(new ProfileStoreTask<Long>() {
            @Override
            public Long execute(ProfileStore store) {
                Sha256Hash imageHash = Sha256Hash.computeHash(IMAGE);
                store.updateImage(userId, imageHash, new ByteArrayInputStream(IMAGE));
                UserProfile storedData = store.getUserData(userId);
                Assert.assertEquals(user, storedData);
                return userId;
            }
        });
        return streamId;
    }

    @Test
    public void testDeleteImage() {
        final long userId = storeUser();
        storeImage(userId);
        runWithRetry(Transaction.TransactionType.AGGRESSIVE_HARD_DELETE, new ProfileStoreTask<Long>() {
            @Override
            public Long execute(ProfileStore store) {
                store.deleteImage(userId);
                return userId;
            }
        });
        txnMgr.runTaskWithRetry(new TransactionTask<Void, RuntimeException>() {
            @Override
            public Void execute(Transaction t) {
                ProfileTableFactory tables = ProfileTableFactory.of();
                UserPhotosStreamValueTable streams = tables.getUserPhotosStreamValueTable(t);
                Assert.assertTrue(streams.getAllRowsUnordered().isEmpty());
                return null;
            }
        });
    }

    @Test
    public void testBirthdayIndex() {
        final long userId = storeUser();
        runWithRetry(new ProfileStoreTask<Long>() {
            @Override
            public Long execute(ProfileStore store) {
                Set<Long> usersWithBirthday = store.getUsersWithBirthday(user.getBirthEpochDay());
                Assert.assertEquals(ImmutableSet.of(userId), usersWithBirthday);
                return userId;
            }
        });
    }

    private Long storeUser() {
        return runWithRetry(new ProfileStoreTask<Long>() {
            @Override
            public Long execute(ProfileStore store) {
                long userId = store.storeNewUser(user);
                UserProfile storedData = store.getUserData(userId);
                Assert.assertEquals(user, storedData);
                return userId;
            }
        });
    }

    protected <T> T runWithRetry(final ProfileStoreTask<T> task) {
        return runWithRetry(Transaction.TransactionType.DEFAULT, task);
    }

    protected <T> T runWithRetry(final Transaction.TransactionType type, final ProfileStoreTask<T> task) {
        return txnMgr.runTaskWithRetry(new TransactionTask<T, RuntimeException>() {
            @Override
            public T execute(Transaction t) {
                t.setTransactionType(type);
                ProfileStore store = new ProfileStore(txnMgr, t);
                return task.execute(store);
            }
        });

    }

}
