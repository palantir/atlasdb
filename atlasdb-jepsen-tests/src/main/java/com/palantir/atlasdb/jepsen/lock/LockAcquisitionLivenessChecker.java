/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.jepsen.lock;

import com.palantir.atlasdb.jepsen.CheckerResult;
import com.palantir.atlasdb.jepsen.events.Checker;
import com.palantir.atlasdb.jepsen.events.Event;
import com.palantir.atlasdb.jepsen.events.RequestType;
import com.palantir.atlasdb.jepsen.utils.LivenessChecker;
import java.util.List;
import java.util.Objects;

public class LockAcquisitionLivenessChecker implements Checker {
    private final LivenessChecker delegate;

    public LockAcquisitionLivenessChecker() {
        delegate = new LivenessChecker(okEvent -> okEvent.function().equals(RequestType.LOCK)
                && !Objects.requireNonNull(okEvent.value()).isEmpty());
    }

    @Override
    public CheckerResult check(List<Event> events) {
        return delegate.check(events);
    }
}
