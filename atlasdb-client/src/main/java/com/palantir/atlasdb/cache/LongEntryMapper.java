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

import com.palantir.atlasdb.cache.DefaultOffHeapCache.EntryMapper;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.logsafe.Preconditions;
import javax.annotation.Nonnull;
import okio.ByteString;

public final class LongEntryMapper implements EntryMapper<Long, Long> {
    @Override
    public ByteString serializeKey(Long key) {
        Preconditions.checkNotNull(key, "Key should not be null");
        return toByteString(key);
    }

    @Override
    public Long deserializeKey(ByteString key) {
        Preconditions.checkNotNull(key, "Key should not be null");
        return toLong(key);
    }

    @Override
    public ByteString serializeValue(Long key, Long value) {
        Preconditions.checkNotNull(key, "Key should not be null");
        Preconditions.checkNotNull(value, "Value should not be null");
        return toByteString(value);
    }

    @Override
    public Long deserializeValue(ByteString key, ByteString value) {
        Preconditions.checkNotNull(key, "Key should not be null");
        Preconditions.checkNotNull(value, "Value should not be null");
        return toLong(value);
    }

    private static ByteString toByteString(@Nonnull Long value) {
        return ByteString.of(ValueType.VAR_LONG.convertFromJava(value));
    }

    private static Long toLong(ByteString value) {
        return (Long) ValueType.VAR_LONG.convertToJava(value.toByteArray(), 0);
    }
}
