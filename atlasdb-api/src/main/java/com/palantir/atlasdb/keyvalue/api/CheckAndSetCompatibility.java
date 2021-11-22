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

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

/**
 * Indicates whether a {@link KeyValueService} supports check and set (CAS) and put unless exists (PUE) operations, and
 * if so the granularity with which it can provide feedback.
 */
public interface CheckAndSetCompatibility {
    /**
     * If false, this {@link KeyValueService} does not support check and set operations.
     */
    boolean supportsCheckAndSetOperations();

    /**
     * If false, there are no guarantees that a {@link CheckAndSetException#getActualValues()} or
     * {@link KeyAlreadyExistsException#getExistingKeys()} from exceptions thrown by the the {@link KeyValueService}
     * will actually return any meaningful data (other than the fact that the operation failed).
     *
     * This method should only be called if {@link CheckAndSetCompatibility#supportsCheckAndSetOperations()} is true.
     * If it is not true, behaviour is undefined.
     */
    boolean supportsDetailOnFailure();

    /**
     *  If true, on CAS or PUE failure other than a {@link CheckAndSetException} or a
     *  {@link KeyAlreadyExistsException}, the values may or may not have been persisted but the state is guaranteed to
     *  be consistent: any subsequent reads will be repeatable. If false, the value may have been persisted
     *  in a way that subsequent reads are not repeatable: if a read before the operation could have returned values
     *  from a set S, it can non-deterministically return a value from S U {newValue} afterwards.
     *
     * This method should only be called if {@link CheckAndSetCompatibility#supportsCheckAndSetOperations()} is true.
     * If it is not true, behaviour is undefined.
     */
    boolean consistentOnFailure();

    static CheckAndSetCompatibility intersect(Stream<CheckAndSetCompatibility> compatibilities) {
        Set<CheckAndSetCompatibility> presentCompatibilities = compatibilities.collect(Collectors.toSet());

        boolean supported =
                presentCompatibilities.stream().allMatch(CheckAndSetCompatibility::supportsCheckAndSetOperations);
        if (!supported) {
            return Unsupported.INSTANCE;
        }
        boolean detail = presentCompatibilities.stream().allMatch(CheckAndSetCompatibility::supportsDetailOnFailure);
        boolean consistency = presentCompatibilities.stream().allMatch(CheckAndSetCompatibility::consistentOnFailure);

        return supportedBuilder()
                .supportsDetailOnFailure(detail)
                .consistentOnFailure(consistency)
                .build();
    }

    static CheckAndSetCompatibility unsupported() {
        return Unsupported.INSTANCE;
    }

    static ImmutableSupported.Builder supportedBuilder() {
        return ImmutableSupported.builder();
    }

    enum Unsupported implements CheckAndSetCompatibility {
        INSTANCE;

        @Override
        public boolean supportsCheckAndSetOperations() {
            return false;
        }

        @Override
        public boolean supportsDetailOnFailure() {
            throw new SafeIllegalStateException("Should not check a KVS that does not support CAS operations for "
                    + "detail");
        }

        @Override
        public boolean consistentOnFailure() {
            throw new SafeIllegalStateException("Should not check a KVS that does not support CAS operations for "
                    + "consistency");
        }
    }

    @Value.Immutable
    abstract class Supported implements CheckAndSetCompatibility {
        @Override
        public boolean supportsCheckAndSetOperations() {
            return true;
        }
    }
}
