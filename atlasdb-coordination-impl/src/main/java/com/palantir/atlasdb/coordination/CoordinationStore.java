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

package com.palantir.atlasdb.coordination;

import java.util.Optional;

import com.palantir.atlasdb.keyvalue.impl.CheckAndSetResult;

/**
 * A {@link CoordinationStore} stores data that a {@link CoordinationService} may use.
 */
public interface CoordinationStore {
    /**
     * Gets the value associated with a given sequence number.
     * Sequence numbers are expected to be strictly positive.
     *
     * @param sequenceNumber sequence number to read a value for
     * @return value stored at that sequence number; empty if none is present
     */
    Optional<byte[]> getValue(long sequenceNumber);

    /**
     * Stores a value for a given sequence number.
     * Sequence numbers are expected to be strictly positive.
     *
     * @param sequenceNumber sequence number to store a value for
     * @param value value to be stored
     */
    void putValue(long sequenceNumber, byte[] value);

    /**
     * Gets the current value of the {@link SequenceAndBound} that a {@link CoordinationService} may have stored.
     *
     * @return available sequence and bound; empty if no sequence and bound has ever been stored
     */
    Optional<SequenceAndBound> getCoordinationValue();

    /**
     * Attempts to atomically update the {@link SequenceAndBound} associated with the relevant
     * {@link CoordinationService}.
     *
     * @param oldValue old value of the sequence and bound
     * @param newValue new value of the sequence and bound
     * @return a {@link CheckAndSetResult} indicating success or failure of the operation, and the current value
     */
    CheckAndSetResult<SequenceAndBound> checkAndSetCoordinationValue(
            Optional<SequenceAndBound> oldValue, SequenceAndBound newValue);
}
