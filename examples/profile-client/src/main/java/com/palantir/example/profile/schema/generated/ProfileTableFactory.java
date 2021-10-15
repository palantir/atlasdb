package com.palantir.example.profile.schema.generated;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.table.generation.Triggers;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.lang.Override;
import java.util.List;
import javax.annotation.Generated;

@Generated("com.palantir.atlasdb.table.description.render.TableFactoryRenderer")
public final class ProfileTableFactory {
    private static final Namespace defaultNamespace = Namespace.create("default", Namespace.UNCHECKED_NAME);

    private final List<Function<? super Transaction, SharedTriggers>> sharedTriggers;

    private final Namespace namespace;

    private ProfileTableFactory(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        this.sharedTriggers = sharedTriggers;
        this.namespace = namespace;
    }

    public static ProfileTableFactory of(
            List<Function<? super Transaction, SharedTriggers>> sharedTriggers, Namespace namespace) {
        return new ProfileTableFactory(sharedTriggers, namespace);
    }

    public static ProfileTableFactory of(List<Function<? super Transaction, SharedTriggers>> sharedTriggers) {
        return new ProfileTableFactory(sharedTriggers, defaultNamespace);
    }

    public static ProfileTableFactory of(Namespace namespace) {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), namespace);
    }

    public static ProfileTableFactory of() {
        return of(ImmutableList.<Function<? super Transaction, SharedTriggers>>of(), defaultNamespace);
    }

    public UserPhotosStreamHashAidxTable getUserPhotosStreamHashAidxTable(
            Transaction t, UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxTrigger... triggers) {
        return UserPhotosStreamHashAidxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserPhotosStreamIdxTable getUserPhotosStreamIdxTable(
            Transaction t, UserPhotosStreamIdxTable.UserPhotosStreamIdxTrigger... triggers) {
        return UserPhotosStreamIdxTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserPhotosStreamMetadataTable getUserPhotosStreamMetadataTable(
            Transaction t, UserPhotosStreamMetadataTable.UserPhotosStreamMetadataTrigger... triggers) {
        return UserPhotosStreamMetadataTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserPhotosStreamValueTable getUserPhotosStreamValueTable(
            Transaction t, UserPhotosStreamValueTable.UserPhotosStreamValueTrigger... triggers) {
        return UserPhotosStreamValueTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public UserProfileTable getUserProfileTable(Transaction t, UserProfileTable.UserProfileTrigger... triggers) {
        return UserProfileTable.of(t, namespace, Triggers.getAllTriggers(t, sharedTriggers, triggers));
    }

    public interface SharedTriggers
            extends UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxTrigger,
                    UserPhotosStreamIdxTable.UserPhotosStreamIdxTrigger,
                    UserPhotosStreamMetadataTable.UserPhotosStreamMetadataTrigger,
                    UserPhotosStreamValueTable.UserPhotosStreamValueTrigger,
                    UserProfileTable.UserProfileTrigger {}

    public abstract static class NullSharedTriggers implements SharedTriggers {
        @Override
        public void putUserPhotosStreamHashAidx(
                Multimap<
                                UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxRow,
                                ? extends UserPhotosStreamHashAidxTable.UserPhotosStreamHashAidxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putUserPhotosStreamIdx(
                Multimap<
                                UserPhotosStreamIdxTable.UserPhotosStreamIdxRow,
                                ? extends UserPhotosStreamIdxTable.UserPhotosStreamIdxColumnValue>
                        newRows) {
            // do nothing
        }

        @Override
        public void putUserPhotosStreamMetadata(
                Multimap<
                                UserPhotosStreamMetadataTable.UserPhotosStreamMetadataRow,
                                ? extends UserPhotosStreamMetadataTable.UserPhotosStreamMetadataNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putUserPhotosStreamValue(
                Multimap<
                                UserPhotosStreamValueTable.UserPhotosStreamValueRow,
                                ? extends UserPhotosStreamValueTable.UserPhotosStreamValueNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }

        @Override
        public void putUserProfile(
                Multimap<UserProfileTable.UserProfileRow, ? extends UserProfileTable.UserProfileNamedColumnValue<?>>
                        newRows) {
            // do nothing
        }
    }
}
