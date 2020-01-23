/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import org.junit.Test;

import com.palantir.atlasdb.table.description.ValueType;

import okio.ByteString;

public final class DeltaEncodingTimestampEntryMapperTests {
    private final OffHeapTimestampCache.EntryMapper<Long, Long> mapper =
            new DeltaEncodingTimestampEntryMapper(new LongEntryMapper());

    @Test
    public void failsOnNulls() {
        assertThatNullPointerException()
                .isThrownBy(() -> mapper.deserializeKey(null));
        assertThatNullPointerException()
                .isThrownBy(() -> mapper.deserializeValue(null, toByteString(4L)));
        assertThatNullPointerException()
                .isThrownBy(() -> mapper.deserializeValue(toByteString(2L), null));
    }

    @Test
    public void encodedValue() {
        assertThat(mapper.deserializeKey(mapper.serializeKey(1L)))
                .isEqualTo(1L);
        assertThat(mapper.deserializeValue(mapper.serializeKey(1L), mapper.serializeValue(1L, 3L)))
                .isEqualTo(3L);
    }

    @Test
    public void valueEncodedAsDelta() {
        assertThat(mapper.serializeValue(1L, 3L))
                .isEqualByComparingTo(toByteString(2L));
    }

    @Test
    public void valueDecodedWithDelta() {
        assertThat(mapper.deserializeValue(toByteString(1L), toByteString(4L)))
                .isEqualTo(5L);
    }

    private static ByteString toByteString(long value) {
        return ByteString.of(ValueType.VAR_LONG.convertFromJava(value));
    }
}
