/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup.transaction;

import com.palantir.atlasdb.pue.PutUnlessExistsValue;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Optional;

public abstract class TransactionTableEntry<T> extends AbstractMap.SimpleImmutableEntry<Long, Optional<T>> {

    private static final long serialVersionUID = 1L;

    public TransactionTableEntry(Map.Entry<? extends Long, ? extends Optional<T>> entry) {
        super(entry);
    }

    public TransactionTableEntry(Long key, Optional<T> value) {
        super(key, value);
    }

    public Long getStartTimestamp() {
        return getKey();
    }

    public Optional<T> getCommitValue() {
        return getValue();
    }

    public abstract Long getCommitTimestampValue();

    public boolean isExplicitlyAborted() {
        return getValue().isEmpty();
    }

    public static class LegacyEntry extends TransactionTableEntry<Long> {
        public LegacyEntry(Long key, Optional<Long> value) {
            super(key, value);
        }

        @Override
        public Long getCommitTimestampValue() {
            return getValue().orElse(null);
        }
    }

    public static class TwoStageEntry extends TransactionTableEntry<PutUnlessExistsValue<Long>> {
        public TwoStageEntry(Long key, Optional<PutUnlessExistsValue<Long>> value) {
            super(key, value);
        }

        @Override
        public Long getCommitTimestampValue() {
            return getValue().map(PutUnlessExistsValue::value).orElse(null);
        }
    }
}
