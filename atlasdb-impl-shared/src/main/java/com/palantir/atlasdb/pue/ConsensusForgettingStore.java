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

package com.palantir.atlasdb.pue;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import java.util.Map;
import java.util.Optional;

/**
 * A table that supports atomic put unless exists and check and touch operations, but explicitly does NOT guarantee
 * repeatable reads in case of failure. For any individual cell the following is guaranteed:
 *   1. A put unless exists can succeed at most once.
 *   2. If a put unless exists fails, this may make future PUE impossible to succeed, but then there must exist a
 *   value for which a check and touch will eventually succeed.
 *   3. Once (1) or (2) is successful, get will never return an Optional.empty() and will always return a consistent
 *   value until a put occurs.
 */
public interface ConsensusForgettingStore {
    /**
     * An atomic put unless exists operation. If this method throws an exception, there are no consistency guarantees:
     *   1. A subsequent PUE may succeed or fail non-deterministically
     *   2. A subsequent get may return Optional.of(value), Optional.empty(), or even Optional.of(other_value) if
     *   another PUE has failed in the past non-deterministically
     */
    void putUnlessExists(Cell cell, byte[] value) throws KeyAlreadyExistsException;

    void putUnlessExists(Map<Cell, byte[]> values) throws KeyAlreadyExistsException;

    /**
     * An atomic operation that verifies the value for a cell. If successful, until a
     * {@link ConsensusForgettingStore#put(Cell, byte[])} is called subsequent gets are guaranteed to return
     * Optional.of(value), and subsequent PUE is guaranteed to throw a KeyAlreadyExistsException.
     */
    void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException;

    default void checkAndTouch(Map<Cell, byte[]> values) throws CheckAndSetException {
        values.forEach(this::checkAndTouch);
    }

    ListenableFuture<Optional<byte[]>> get(Cell cell);

    ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells);

    /**
     * A put operation that offers no consistency guarantees when an exception is thrown. Multiple puts into the same
     * cell with different values may result in non-repeatable reads.
     */
    void put(Cell cell, byte[] value);

    void put(Map<Cell, byte[]> values);
}
