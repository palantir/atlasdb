/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.transaction.impl.TransactionTables;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.Test;

public class AbandonedTimestampStoreImplTest {
    private static final long TIMESTAMP = 4L;

    private final KeyValueService keyValueService = new InMemoryKeyValueService(false);
    private final AbandonedTimestampStore abandonedTimestampStore = new AbandonedTimestampStoreImpl(keyValueService);

    @Before
    public void before() {
        TransactionTables.createTables(keyValueService);
    }

    @Test
    public void markingTimestampsAbandonedIsIdempotent() {
        abandonedTimestampStore.markAbandoned(TIMESTAMP);
        assertThatCode(() -> abandonedTimestampStore.markAbandoned(TIMESTAMP)).doesNotThrowAnyException();
    }

    @Test
    public void timestampsMarkedAsAbandonedCanBeRetrievedFromTheStore() {
        abandonedTimestampStore.markAbandoned(TIMESTAMP);
        assertThat(abandonedTimestampStore.getAbandonedTimestampsInRange(TIMESTAMP, TIMESTAMP))
                .containsExactly(TIMESTAMP);
    }

    @Test
    public void onlyReturnsTimestampsInProvidedRange() {
        abandonedTimestampStore.markAbandoned(TIMESTAMP);
        abandonedTimestampStore.markAbandoned(TIMESTAMP + 5);
        abandonedTimestampStore.markAbandoned(TIMESTAMP + 10);
        assertThat(abandonedTimestampStore.getAbandonedTimestampsInRange(TIMESTAMP, TIMESTAMP + 7))
                .containsExactly(TIMESTAMP, TIMESTAMP + 5);
    }

    @Test
    public void onlyTimestampsRequestedForAreActuallyReturned() {
        int timestampLimit = 100;
        IntStream.range(0, timestampLimit).forEach(abandonedTimestampStore::markAbandoned);

        for (int start = 0; start < timestampLimit; start++) {
            for (int end = start; end < timestampLimit; end++) {
                assertThat(abandonedTimestampStore.getAbandonedTimestampsInRange(start, end))
                        .containsExactlyElementsOf(
                                LongStream.rangeClosed(start, end).boxed().collect(Collectors.toSet()));
            }
        }
    }

    @Test
    public void throwsOnRequestingForInvalidTimestampRange() {
        assertThatThrownBy(() -> abandonedTimestampStore.getAbandonedTimestampsInRange(TIMESTAMP, TIMESTAMP - 2))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessageContaining("Range provided does not exist");
    }
}
