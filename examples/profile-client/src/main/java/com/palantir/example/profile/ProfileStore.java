/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.example.profile;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.transaction.api.Transaction;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.common.collect.IterableView;
import com.palantir.example.profile.protos.generated.ProfilePersistence.UserProfile;
import com.palantir.example.profile.schema.generated.ProfileTableFactory;
import com.palantir.example.profile.schema.generated.UserPhotosStreamStore;
import com.palantir.example.profile.schema.generated.UserProfileTable;
import com.palantir.example.profile.schema.generated.UserProfileTable.UserBirthdaysIdxTable;
import com.palantir.example.profile.schema.generated.UserProfileTable.UserBirthdaysIdxTable.UserBirthdaysIdxColumn;
import com.palantir.example.profile.schema.generated.UserProfileTable.UserBirthdaysIdxTable.UserBirthdaysIdxColumnValue;
import com.palantir.example.profile.schema.generated.UserProfileTable.UserBirthdaysIdxTable.UserBirthdaysIdxRow;
import com.palantir.example.profile.schema.generated.UserProfileTable.UserProfileRow;
import com.palantir.util.crypto.Sha256Hash;

public class ProfileStore {
    final TransactionManager txnMgr;
    final Transaction t;
    final ProfileTableFactory tables = ProfileTableFactory.of();

    public ProfileStore(TransactionManager txnMgr, Transaction t) {
        this.txnMgr = txnMgr;
        this.t = t;
    }

    private long getNewId() {
        long result = -1;
        while (result < 0) {
            result = new Random().nextLong();
        }
        return result;
    }

    public long storeNewUser(UserProfile data) {
        long userId = getNewId();
        UserProfileTable table = tables.getUserProfileTable(t);
        table.putMetadata(UserProfileRow.of(userId), data);
        return userId;
    }

    public UserProfile getUserData(long userId) {
        UserProfileTable table = tables.getUserProfileTable(t);
        Map<UserProfileRow, UserProfile> result = table.getMetadatas(ImmutableSet.of(UserProfileRow.of(userId)));
        if (result.isEmpty()) {
            return null;
        } else {
            return Iterables.getOnlyElement(result.values());
        }
    }

    private Long getPhotoStreamId(long userId) {
        UserProfileTable table = tables.getUserProfileTable(t);
        Map<UserProfileRow, Long> result = table.getPhotoStreamIds(ImmutableSet.of(UserProfileRow.of(userId)));
        if (result.isEmpty()) {
            return null;
        } else {
            return Iterables.getOnlyElement(result.values());
        }
    }

    public InputStream getImageForUser(long userId) {
        Long photoId = getPhotoStreamId(userId);
        if (photoId == null) {
            return null;
        }
        UserPhotosStreamStore streamStore = UserPhotosStreamStore.of(txnMgr, tables);
        return streamStore.loadStream(t, photoId);
    }

    public void updateImage(long userId, Sha256Hash hash, InputStream imageData) {
        UserProfile userData = getUserData(userId);
        Preconditions.checkNotNull(userData);

        UserPhotosStreamStore streamStore = UserPhotosStreamStore.of(txnMgr, tables);
        Long oldStreamId = getPhotoStreamId(userId);
        if (oldStreamId != null) {
            // Unmark old stream before we overwrite it.
            streamStore.unmarkStreamAsUsed(t, oldStreamId, PtBytes.toBytes(userId));
        }

        // This will either store a new stream and mark it as used or return an old stream that matches the hash and mark it as used.
        long streamId = streamStore.getByHashOrStoreStreamAndMarkAsUsed(t, hash, imageData, PtBytes.toBytes(userId));

        UserProfileTable table = tables.getUserProfileTable(t);
        table.putPhotoStreamId(UserProfileRow.of(userId), streamId);
    }

    public void deleteImage(long userId) {
        Long streamId = getPhotoStreamId(userId);
        if (streamId == null) {
            return;
        }
        UserProfileTable table = tables.getUserProfileTable(t);
        table.deletePhotoStreamId(UserProfileRow.of(userId));

        UserPhotosStreamStore streamStore = UserPhotosStreamStore.of(txnMgr, tables);
        streamStore.unmarkStreamAsUsed(t, streamId, PtBytes.toBytes(userId));
    }

    public Set<Long> getUsersWithBirthday(long birthEpochDays) {
        UserBirthdaysIdxTable table = UserBirthdaysIdxTable.of(t);
        List<UserBirthdaysIdxColumnValue> columns = table.getRowColumns(UserBirthdaysIdxRow.of(birthEpochDays));

        return IterableView.of(columns)
		        .transform(UserBirthdaysIdxColumnValue.getColumnNameFun())
		        .transform(UserBirthdaysIdxColumn.getIdFun())
		        .immutableSetCopy();
    }

}
