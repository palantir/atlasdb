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

import com.google.protobuf.ByteString;
import com.palantir.atlasdb.cache.DefaultOffHeapCache.EntryMapper;
import com.palantir.logsafe.Preconditions;

public final class DeltaEncodingTimestampEntryMapper implements EntryMapper<Long, Long> {
    private final EntryMapper<Long, Long> entryMapper;

    public DeltaEncodingTimestampEntryMapper(EntryMapper<Long, Long> entryMapper) {
        this.entryMapper = entryMapper;
    }

    @Override
    public ByteString serializeKey(Long key) {
        Preconditions.checkNotNull(key);
        return entryMapper.serializeKey(key);
    }

    @Override
    public Long deserializeKey(ByteString key) {
        Preconditions.checkNotNull(key);
        return entryMapper.deserializeKey(key);
    }

    @Override
    public ByteString serializeValue(Long key, Long value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        return entryMapper.serializeValue(key, value - key);
    }

    @Override
    public Long deserializeValue(ByteString key, ByteString value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        return deserializeKey(key) + entryMapper.deserializeValue(key, value);
    }
}
