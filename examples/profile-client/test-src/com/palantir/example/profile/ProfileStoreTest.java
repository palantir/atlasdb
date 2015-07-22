package com.palantir.example.profile;

import java.io.ByteArrayInputStream;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.memory.InMemoryAtlasDb;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.api.TransactionTask;
import com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile;
import com.palantir.example.profile.schema.ProfileSchema;
import com.palantir.util.crypto.Sha256Hash;

public class ProfileStoreTest {
    TransactionManager txnMgr;
    UserProfile user = UserProfile.newBuilder().setBirthEpochDay(0).setName("first last").build();

    @Before
    public void setup() {
        txnMgr = InMemoryAtlasDb.createInMemoryTransactionManager(ProfileSchema.INSTANCE);
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
        runWithRetry(new ProfileStoreTask<Long>() {
            @Override
            public Long execute(ProfileStore store) {
                byte[] image = new byte[] {0, 1, 2, 3};
                store.updateImage(userId, Sha256Hash.computeHash(image), new ByteArrayInputStream(image));
                UserProfile storedData = store.getUserData(userId);
                Assert.assertEquals(user, storedData);
                return userId;
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
        return txnMgr.runTaskWithRetry(new TransactionTask<T, RuntimeException>() {
            @Override
            public T execute(Transaction t) {
                ProfileStore store = new ProfileStore(txnMgr, t);
                return task.execute(store);
            }
        });

    }

}
