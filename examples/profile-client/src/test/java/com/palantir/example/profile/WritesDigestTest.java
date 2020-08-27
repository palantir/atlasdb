/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ImmutableAtlasDbRuntimeConfig;
import com.palantir.atlasdb.debug.WritesDigest;
import com.palantir.atlasdb.debug.WritesDigestEmitter;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.example.profile.schema.ProfileSchema;
import com.palantir.example.profile.schema.generated.ProfileTableFactory;
import com.palantir.example.profile.schema.generated.UserProfileTable;
import com.palantir.example.profile.schema.generated.UserProfileTable.PhotoStreamId;
import com.palantir.example.profile.schema.generated.UserProfileTable.UserProfileRow;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;

public class WritesDigestTest {

    private static final ProfileTableFactory TABLE_FACTORY = ProfileTableFactory.of();
    private TransactionManager transactionManager;
    private WritesDigestEmitter emitter;

    @Before
    public void setUp() {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder().keyValueService(new InMemoryAtlasDbConfig()).build();
        AtlasDbRuntimeConfig runtimeConfig = ImmutableAtlasDbRuntimeConfig.withSweepDisabled();
        transactionManager = TransactionManagers.builder()
                .config(config)
                .userAgent(AtlasDbRemotingConstants.DEFAULT_USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .addSchemas(ProfileSchema.INSTANCE.getLatestSchema())
                .runtimeConfigSupplier(() -> Optional.of(runtimeConfig))
                .build()
                .serializable();
        emitter = new WritesDigestEmitter(transactionManager, TABLE_FACTORY.getUserProfileTable(null).getTableRef());

    }

    @Test
    public void canGetAllWritesThatHaventBeenSwept() {
        UUID uuid = UUID.randomUUID();
        UserProfileRow row = UserProfileRow.of(uuid);
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 5L)));
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 11L)));
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 19L)));
        runWithRetryVoid(store -> store.putPhotoStreamId(ImmutableMap.of(row, 23L)));

        WritesDigest<String> digest = emitter.getDigest(row, PhotoStreamId.of(0L).persistColumnName());

        assertThat(digest.allWrittenTimestamps())
                .as("nothing should be swept, we can see all the timestamps we've written at thus far")
                .hasSize(4);

        assertThat(digest.allWrittenValuesDeserialized().values())
                .as("nothing should be swept, we can see all the values we've written thus far")
                .containsOnly(
                        base64PhotoStreamId(5L),
                        base64PhotoStreamId(11L),
                        base64PhotoStreamId(19L),
                        base64PhotoStreamId(23L));

        System.out.println(digest);
    }

    private <T> T runWithRetry(Function<UserProfileTable, T> task) {
        return transactionManager.runTaskWithRetry(transaction ->
                task.apply(TABLE_FACTORY.getUserProfileTable(transaction)));
    }

    private void runWithRetryVoid(Consumer<UserProfileTable> task) {
        runWithRetry(store -> {
            task.accept(store);
            return null;
        });
    }

    private static String base64PhotoStreamId(long id) {
        return BaseEncoding.base64().encode(PhotoStreamId.of(id).persistValue());
    }
}
