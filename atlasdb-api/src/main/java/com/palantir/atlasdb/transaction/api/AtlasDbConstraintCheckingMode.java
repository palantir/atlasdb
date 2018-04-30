/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.transaction.api;

public enum AtlasDbConstraintCheckingMode {
    FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS(ShouldThrow.YES),
    FULL_CONSTRAINT_CHECKING_LOGS_EXCEPTIONS(ShouldThrow.NO),
    PERFORMANCE_OPTIMAL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS(ShouldThrow.YES),
    PERFORMANCE_OPTIMAL_CONSTRAINT_CHECKING_LOGS_EXCEPTIONS(ShouldThrow.NO),
    NO_CONSTRAINT_CHECKING(ShouldThrow.NO);

    private enum ShouldThrow {
        YES,
        NO
    }

    private final ShouldThrow shouldThrow;

    AtlasDbConstraintCheckingMode(ShouldThrow shouldThrow) {
        this.shouldThrow = shouldThrow;
    }

    public boolean shouldThrowException() {
        return shouldThrow == ShouldThrow.YES;
    }
}
