/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.persist.api;

import com.palantir.common.persist.Persistable.Hydrator;

/**
 * {@link ReusablePersister}s are required to have a no arg constructor and be thread-safe. It will be re-used across
 * executions.
 *
 * If persisters need state while (de)serializing, create a (de)serializer class and instantiate it in the relevant
 * hydrate/persist method.
 */
public interface ReusablePersister<T> extends Hydrator<T> {
    byte[] persistToBytes(T objectToPersist);

    Class<T> getPersistingClassType();
}
