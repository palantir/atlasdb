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

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Indicates whether a {@link KeyValueService} supports check and set (CAS) and put unless exists (PUE) operations, and
 * if so the granularity with which it can provide feedback.
 */
public enum CheckAndSetCompatibility {
    NOT_SUPPORTED,
    /**
     * The {@link KeyValueService} supports CAS and PUE operations. However, in the event of failure, there are no
     * guarantees that {@link CheckAndSetException#getActualValues()} or
     * {@link KeyAlreadyExistsException#getExistingKeys()} actually return any meaningful data (other than the
     * fact that the operation failed).
     */
    SUPPORTED_NO_DETAIL_ON_FAILURE,
    /**
     * The {@link KeyValueService} supports CAS and PUE operations. In the event of failure:
     *
     * - CAS: {@link CheckAndSetException#getActualValues()} on any such exception thrown must return the list
     *        of existing values. (In practice, this should have zero or one elements.)
     * - PUE: {@link KeyAlreadyExistsException#getExistingKeys()} on any such exception thrown must return the list
     *        of all pre-existing cells for any row which the implementation attempted to put into the key value
     *        service. Note that there is no guarantee that the implementation attempts to put all rows atomically.
     * - Other failure: values may have persisted partially, possibly causing non-repeatable reads.
     */
    SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST;

    public static CheckAndSetCompatibility min(Stream<CheckAndSetCompatibility> compatibilities) {
        Set<CheckAndSetCompatibility> presentCompatibilities = compatibilities.collect(Collectors.toSet());
        return Stream.of(
                        NOT_SUPPORTED,
                        SUPPORTED_NO_DETAIL_ON_FAILURE,
                        SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST)
                .filter(presentCompatibilities::contains)
                .findFirst()
                .orElseThrow(() -> new SafeIllegalArgumentException("min requires at least 1 element, but 0 provided"));
    }
}
