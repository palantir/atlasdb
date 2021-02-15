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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.cache.DefaultOffHeapCache.EntryMapper;
import com.palantir.atlasdb.table.description.ValueType;
import okio.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class DeltaEncodingTimestampEntryMapperTests {
    @Mock
    public EntryMapper<Long, Long> longEntryMapper;

    private EntryMapper<Long, Long> mapper;

    @Before
    public void setUp() {
        mapper = new DeltaEncodingTimestampEntryMapper(longEntryMapper);
    }

    @Test
    public void failsOnNulls() {
        assertThatNullPointerException().isThrownBy(() -> mapper.deserializeKey(null));
        assertThatNullPointerException().isThrownBy(() -> mapper.deserializeValue(null, toByteString(4L)));
        assertThatNullPointerException().isThrownBy(() -> mapper.deserializeValue(toByteString(2L), null));
    }

    @Test
    public void valueEncodedAsDelta() {
        mapper.serializeValue(50023423423423567L, 50023423423423570L);

        verify(longEntryMapper, times(1)).serializeValue(50023423423423567L, 3L);
    }

    @Test
    public void valueDecodedWithDelta() {
        when(longEntryMapper.deserializeValue(toByteString(50023423423423567L), toByteString(4L)))
                .thenReturn(4L);
        when(longEntryMapper.deserializeKey(toByteString(50023423423423567L))).thenReturn(50023423423423567L);

        assertThat(mapper.deserializeValue(toByteString(50023423423423567L), toByteString(4L)))
                .isEqualTo(50023423423423571L);
    }

    private static ByteString toByteString(long value) {
        return ByteString.of(ValueType.VAR_LONG.convertFromJava(value));
    }
}
