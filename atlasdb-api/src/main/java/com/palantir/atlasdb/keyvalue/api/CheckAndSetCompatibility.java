/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

/**
 * Indicates whether a {@link KeyValueService} supports check and set (CAS) and put unless exists (PUE) operations, and
 * if so the granularity with which it can provide feedback.
 */
public enum CheckAndSetCompatibility {
    NOT_SUPPORTED,
    /**
     * The {@link KeyValueService} supports CAS and PUE operations. In the event of failure:
     *
     * - CAS: {@link CheckAndSetException#getActualValues()} on any such exception thrown will return a list of
     *        values that existed in the database at some point during the CAS operation. Notice that this may
     *        even match the value of the {@link CheckAndSetRequest#oldValue()} in ABA situations.
     * - PUE: {@link KeyAlreadyExistsException#getExistingKeys()} must return the list of pre-existing cells
     *        for any row which the implementation attempted to put into the key value service, as read at some
     *        point during the PUE operation. These reads need not be atomic, and it is possible to get empty
     *        results (if a value is separately added and then deleted during the PUE operation).
     */
    SUPPORTED_DETAIL_ON_FAILURE_NOT_ATOMIC,
    /**
     * The {@link KeyValueService} supports CAS and PUE operations. In the event of failure:
     *
     * - CAS: {@link CheckAndSetException#getActualValues()} on any such exception thrown must return the list
     *        of existing values that, at the database level, caused the CAS operation to fail.
     *        (In practice, this should have zero or one elements.)
     * - PUE: {@link KeyAlreadyExistsException#getExistingKeys()} on any such exception thrown must return the list
     *        of all pre-existing cells for any row which the implementation attempted to put into the key value
     *        service. This list must be current at the time the implementation attempted to put values in to the
     *        key value service for the relevant row. Note that there is no guarantee that the implementation attempts
     *        to put all rows atomically.
     */
    SUPPORTED_DETAIL_ON_FAILURE_ATOMIC;

    public static CheckAndSetCompatibility min(Stream<CheckAndSetCompatibility> compatibilities) {
        Set<CheckAndSetCompatibility> presentCompatibilities = compatibilities.collect(Collectors.toSet());
        return Stream.of(NOT_SUPPORTED, SUPPORTED_DETAIL_ON_FAILURE_NOT_ATOMIC, SUPPORTED_DETAIL_ON_FAILURE_ATOMIC)
                .filter(presentCompatibilities::contains)
                .findFirst()
                .orElseThrow(() -> new SafeIllegalArgumentException("min requires at least 1 element, but 0 provided"));
    }
}
