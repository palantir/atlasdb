/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.precommit;

import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import java.util.Set;

public class DefaultLockValidityChecker implements LockValidityChecker {
    private final TimelockService timelockService;

    public DefaultLockValidityChecker(TimelockService timelockService) {
        this.timelockService = timelockService;
    }

    @Override
    public Set<LockToken> getStillValidLockTokens(Set<LockToken> tokensToRefresh) {
        return timelockService.refreshLockLeases(tokensToRefresh);
    }
}
