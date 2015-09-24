package com.palantir.example.profile.schema.generated;

import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;

public class ProfileTableFactory {
    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    public static ProfileTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new ProfileTableFactory(sharedTriggers);
    }

    private ProfileTableFactory(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        this.sharedTriggers = sharedTriggers;
    }

    public static ProfileTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of());
    }

    public UserPhotosStreamHashAidxTable getUserPhotosStreamHashAidxTable(Transaction t, UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxTrigger... triggers) {
        return UserPhotosStreamHashAidxTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserPhotosStreamIdxTable getUserPhotosStreamIdxTable(Transaction t, UserPhotosStreamIdxTable.UserPhotosStreamIdxTrigger... triggers) {
        return UserPhotosStreamIdxTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserPhotosStreamMetadataTable getUserPhotosStreamMetadataTable(Transaction t, UserPhotosStreamMetadataTable.UserPhotosStreamMetadataTrigger... triggers) {
        return UserPhotosStreamMetadataTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserPhotosStreamValueTable getUserPhotosStreamValueTable(Transaction t, UserPhotosStreamValueTable.UserPhotosStreamValueTrigger... triggers) {
        return UserPhotosStreamValueTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserProfileTable getUserProfileTable(Transaction t, UserProfileTable.UserProfileTrigger... triggers) {
        return UserProfileTable.of(t, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers extends
            UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxTrigger,
            UserPhotosStreamIdxTable.UserPhotosStreamIdxTrigger,
            UserPhotosStreamMetadataTable.UserPhotosStreamMetadataTrigger,
            UserPhotosStreamValueTable.UserPhotosStreamValueTrigger,
            UserProfileTable.UserProfileTrigger {
        /* empty */
    }

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putUserPhotosStreamHashAidx(Multimap<UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow, ? extends UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putUserPhotosStreamIdx(Multimap<UserPhotosStreamIdxTable.UserPhotosStreamIdxRow, ? extends UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue> newRows) {
            // do nothing
        }

        @Override
        public void putUserPhotosStreamMetadata(Multimap<UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow, ? extends UserPhotosStreamMetadataTable.UserPhotosStreamMetadataNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putUserPhotosStreamValue(Multimap<UserPhotosStreamValueTable.UserPhotosStreamValueRow, ? extends UserPhotosStreamValueTable.UserPhotosStreamValueNamedColumnValue<?>> newRows) {
            // do nothing
        }

        @Override
        public void putUserProfile(Multimap<UserProfileTable.UserProfileRow, ? extends UserProfileTable.UserProfileNamedColumnValue<?>> newRows) {
            // do nothing
        }
    }
}