/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.ObjectPersister.LogSafety;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public final class TimestampRangePersisterTest {
    private static final ObjectPersister<TimestampRange> PERSISTER =
            ObjectPersister.of(ObjectMappers.newServerSmileMapper(), TimestampRange.class, LogSafety.SAFE);

    private static final TimestampRange OPEN_RANGE = TimestampRange.openBucket(123L);
    private static final TimestampRange CLOSED_RANGE = TimestampRange.of(123L, 456L);

    // Be very careful about changing these without a migration.
    private static final byte[] SERIALIZED_OPEN_RANGE =
            BaseEncoding.base64().decode("OikKBfqNc3RhcnRJbmNsdXNpdmUkA7aLZW5kRXhjbHVzaXZlwfs");
    private static final byte[] SERIALIZED_CLOSED_RANGE =
            BaseEncoding.base64().decode("OikKBfqNc3RhcnRJbmNsdXNpdmUkA7aLZW5kRXhjbHVzaXZlJA6Q+w==");

    @ParameterizedTest
    @MethodSource("timestampRanges")
    public void deserializingTimestampRangeIsInverseOfSerialization(TimestampRange range) {
        byte[] serialized = PERSISTER.trySerialize(range);
        assertThat(PERSISTER.tryDeserialize(serialized)).isEqualTo(range);
    }

    @ParameterizedTest
    @MethodSource("timestampRanges")
    public void canDeserializeExistingVersionOfTimestampRange(TimestampRange range, byte[] serialized) {
        assertThat(PERSISTER.tryDeserialize(serialized)).isEqualTo(range);
    }

    private static Stream<Arguments> timestampRanges() {
        return Stream.of(
                Arguments.of(OPEN_RANGE, SERIALIZED_OPEN_RANGE), Arguments.of(CLOSED_RANGE, SERIALIZED_CLOSED_RANGE));
    }
}
