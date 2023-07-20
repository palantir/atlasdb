/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.util.io;

import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;

public enum AvailabilityRequirement {
    ANY {
        @Override
        protected int calculateRequired(int total) {
            return 1;
        }
    },
    QUORUM {
        @Override
        protected int calculateRequired(int total) {
            return (total / 2) + 1;
        }
    };

    protected abstract int calculateRequired(int total);

    public final boolean satisfies(int available, int total) {
        Preconditions.checkArgument(
                available >= 0 && total >= 0,
                "Available and total must be non-negative.",
                SafeArg.of("available", available),
                SafeArg.of("total", total));
        Preconditions.checkArgument(
                available <= total,
                "Available must be less than or equal to total.",
                SafeArg.of("available", available),
                SafeArg.of("total", total));
        return available >= calculateRequired(total);
    }
}
