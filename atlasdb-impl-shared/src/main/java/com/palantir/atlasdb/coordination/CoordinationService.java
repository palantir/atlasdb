/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.coordination;

/**
 * Coordinates state concerning internal schema versions and metadata used by AtlasDB.
 *
 * Users are expected to provide state objects that are JSON-serializable.
 */
public interface CoordinationService {
    /**
     * Retrieves the data currently associated with the provided coordinationKey.
     *
     * @param coordinationKey Key used for coordinating.
     * @param metadataType Metadata type, required for deserialization.
     * @param <T> Type of the object being serialized.
     * @return data associated with the coordination key.
     */
    <T> T get(String coordinationKey, Class<T> metadataType);

    <T> void putUnlessExists(String coordinationKey, T desiredValue);

    <T> void checkAndSet(String coordinationKey, T oldValue, T newValue);
}
