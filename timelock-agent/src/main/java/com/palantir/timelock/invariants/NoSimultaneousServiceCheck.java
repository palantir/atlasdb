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

package com.palantir.timelock.invariants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.timelock.TimeLockResource;

public class NoSimultaneousServiceCheck {
    private static final Logger log = LoggerFactory.getLogger(NoSimultaneousServiceCheck.class);

    private static final int REQUIRED_CONSECUTIVE_VIOLATIONS_BEFORE_FAIL = 5;

    private final TimeLockResource timeLockResource;

    public NoSimultaneousServiceCheck(TimeLockResource timeLockResource) {
        this.timeLockResource = timeLockResource;
    }

    public void performCheckOnSpecificClient(String client) {

    }
}
