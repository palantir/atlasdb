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

package com.palantir.atlasdb.timelock.management;

import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.function.Function;

public final class DisableNamespacesResponseVisitors {
    private DisableNamespacesResponseVisitors() {
        // don't even think about it
    }

    public static DisableNamespacesResponse.Visitor<Boolean> successfulDisableVisitor() {
        return of(_unused -> true, _unused -> false);
    }

    public static <T> DisableNamespacesResponse.Visitor<T> of(
            Function<SuccessfulDisableNamespacesResponse, T> visitSuccessful,
            Function<UnsuccessfulDisableNamespacesResponse, T> visitUnsuccessful) {
        return new DisableNamespacesResponse.Visitor<>() {
            @Override
            public T visitSuccessful(SuccessfulDisableNamespacesResponse value) {
                return visitSuccessful.apply(value);
            }

            @Override
            public T visitUnsuccessful(UnsuccessfulDisableNamespacesResponse value) {
                return visitUnsuccessful.apply(value);
            }

            @Override
            public T visitUnknown(String unknownType) {
                throw new SafeIllegalStateException(
                        "Unknown DisabledNamespacesResponse", SafeArg.of("responseType", unknownType));
            }
        };
    }
}
