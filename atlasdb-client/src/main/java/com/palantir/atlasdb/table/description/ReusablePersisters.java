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

package com.palantir.atlasdb.table.description;

import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.atlasdb.persist.api.ReusablePersister;

final class ReusablePersisters {
    private ReusablePersisters() {}

    static <T> Persister<T> backcompat(ReusablePersister<T> persister) {
        return new BackcompatPersister<T>(persister);
    }

    static <T> ReusablePersister<T> wrapLegacyPersister(Persister<T> legacyPersister) {
        return new WrapLegacyPersister<T>(legacyPersister);
    }

    private static final class BackcompatPersister<T> implements Persister<T> {
        private final ReusablePersister<T> delegate;

        private BackcompatPersister(ReusablePersister<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] persistToBytes(T objectToPersist) {
            return delegate.persistToBytes(objectToPersist);
        }

        @Override
        public Class<T> getPersistingClassType() {
            return delegate.getPersistingClassType();
        }

        @Override
        public T hydrateFromBytes(byte[] input) {
            return delegate.hydrateFromBytes(input);
        }
    }

    private static final class WrapLegacyPersister<T> implements ReusablePersister<T> {
        private final Persister<T> delegate;

        private WrapLegacyPersister(Persister<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] persistToBytes(T objectToPersist) {
            return delegate.persistToBytes(objectToPersist);
        }

        @Override
        public Class<T> getPersistingClassType() {
            return delegate.getPersistingClassType();
        }

        @Override
        public T hydrateFromBytes(byte[] input) {
            return delegate.hydrateFromBytes(input);
        }
    }
}
