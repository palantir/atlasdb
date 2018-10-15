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
package com.palantir.common.persist;

/**
 * Classes implementing Persistable must also have a static field called <code>BYTES_HYDRATOR</code>
 * that implements the {@link Persistable.Hydrator} interface. It is also recommened that each
 * {@link Persistable} class is a final class.
 */
public interface Persistable {
    public static final String HYDRATOR_NAME = "BYTES_HYDRATOR";

    public interface Hydrator<T> {
        T hydrateFromBytes(byte[] input);
    }

    byte[] persistToBytes();
}
