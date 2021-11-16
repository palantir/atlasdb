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
    /**
     * This is the only value that is allowed if a {@link KeyValueService} does NOT support check and set.
     */
    NO_DETAIL_CONSISTENT_ON_FAILURE(false, true),

    SUPPORTS_DETAIL_CONSISTENT_ON_FAILURE(true, true),

    SUPPORTS_DETAIL_NOT_CONSISTENT_ON_FAILURE(true, false),

    NO_DETAIL_NOT_CONSISTENT_ON_FAILURE(false, false);

    private final boolean supportsDetailOnFailure;
    private final boolean consistentOnFailure;

    CheckAndSetCompatibility(boolean supportsDetailOnFailure, boolean consistentOnFailure) {
        this.supportsDetailOnFailure = supportsDetailOnFailure;
        this.consistentOnFailure = consistentOnFailure;
    }

    /**
     * If false, there are no guarantees that a {@link CheckAndSetException#getActualValues()} or
     * {@link KeyAlreadyExistsException#getExistingKeys()} from exceptions thrown by the the {@link KeyValueService}
     * will actually return any meaningful data (other than the fact that the operation failed).
     */
    public boolean supportsDetailOnFailure() {
        return supportsDetailOnFailure;
    }

    /**
     *  If true, on CAS or PUE failure other than a {@link CheckAndSetException} or a
     *  {@link KeyAlreadyExistsException}, the values may or may not have been persisted but the state is guaranteed to
     *  be consistent: any subsequent reads will be repeatable. If false, the value may have been persisted
     *  in a way that subsequent reads are not repeatable: if a read before the operation could have returned values
     *  from a set S, it can non-deterministically return a value from S U {newValue} afterwards.
     */
    public boolean consistentOnFailure() {
        return consistentOnFailure;
    }

    public static CheckAndSetCompatibility intersect(Stream<CheckAndSetCompatibility> compatibilities) {
        Set<CheckAndSetCompatibility> presentCompatibilities = compatibilities.collect(Collectors.toSet());
        boolean detail = presentCompatibilities.stream().allMatch(CheckAndSetCompatibility::supportsDetailOnFailure);
        boolean consistency = presentCompatibilities.stream().allMatch(CheckAndSetCompatibility::consistentOnFailure);

        return Stream.of(CheckAndSetCompatibility.values())
                .filter(compatibility -> compatibility.supportsDetailOnFailure() == detail)
                .filter(compatibility -> compatibility.consistentOnFailure() == consistency)
                .findFirst()
                .orElseThrow(() -> new SafeIllegalArgumentException("min requires at least 1 element, but 0 provided"));
    }
}
