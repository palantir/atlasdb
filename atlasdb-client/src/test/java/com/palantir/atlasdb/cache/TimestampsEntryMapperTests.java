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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.exceptions.SafeNullPointerException;

import okio.ByteString;

public final class TimestampsEntryMapperTests {
    private static final ByteString KEY = ByteString.of(ValueType.VAR_LONG.convertFromJava(1L));
    private TimestampsEntryMapper timestampsEntryMapper = new TimestampsEntryMapper();

    @Test
    public void keyCorrectlyMapped() {
        ByteString serializedKey = timestampsEntryMapper.serializeKey(1L);
        assertThat(serializedKey)
                .isEqualByComparingTo(KEY);
        assertThat(timestampsEntryMapper.deserializeKey(KEY))
                .isEqualTo(1L);
    }

    @Test
    public void failsOnNulls() {
        assertThatThrownBy(() -> timestampsEntryMapper.deserializeKey(null))
                .isInstanceOf(SafeNullPointerException.class);
        assertThatThrownBy(() -> timestampsEntryMapper.deserializeValue(null, ByteString.encodeUtf8("bla")))
                .isInstanceOf(SafeNullPointerException.class);
        assertThatThrownBy(() -> timestampsEntryMapper.deserializeValue(1L, null))
                .isInstanceOf(SafeNullPointerException.class);
    }

    @Test
    public void deltaEncoded() {
        ByteString value = timestampsEntryMapper.serializeValue(1L, 3L);
        assertThat(timestampsEntryMapper.deserializeValue(2L, value))
                .isEqualTo(4L);
    }
}
