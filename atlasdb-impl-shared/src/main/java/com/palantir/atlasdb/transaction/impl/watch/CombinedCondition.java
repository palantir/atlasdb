/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.watch;

import com.palantir.atlasdb.transaction.api.PreCommitCondition;

final class CombinedCondition
        implements PreCommitCondition {

    private final PreCommitCondition providedCondition;
    private final WatchPreCommitCondition appendedCondition;

    public CombinedCondition(PreCommitCondition providedCondition, WatchPreCommitCondition appendedCondition) {
        this.providedCondition = providedCondition;
        this.appendedCondition = appendedCondition;
    }

    @Override
    public void throwIfConditionInvalid(long timestamp) {
        providedCondition.throwIfConditionInvalid(timestamp);
        appendedCondition.throwIfConditionInvalid(timestamp);
    }

    @Override
    public void cleanup() {
        providedCondition.cleanup();
        appendedCondition.cleanup();
    }

    public WatchPreCommitCondition getSecondCondition() {
        return appendedCondition;
    }
}
