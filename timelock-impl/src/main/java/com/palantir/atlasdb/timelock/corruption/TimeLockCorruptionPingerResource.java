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

package com.palantir.atlasdb.timelock.corruption;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.timelock.corruption.TimeLockCorruptionPinger;
import com.palantir.timelock.corruption.TimeLockCorruptionPingerEndpoints;
import com.palantir.timelock.corruption.UndertowTimeLockCorruptionPinger;
import com.palantir.tokens.auth.AuthHeader;

public final class TimeLockCorruptionPingerResource implements UndertowTimeLockCorruptionPinger {
    private TimeLockCorruptionState timeLockCorruptionState;

    private TimeLockCorruptionPingerResource(TimeLockCorruptionState timeLockCorruptionState) {
        this.timeLockCorruptionState = timeLockCorruptionState;
    }

    public static UndertowService undertow(TimeLockCorruptionState check) {
        return TimeLockCorruptionPingerEndpoints.of(new TimeLockCorruptionPingerResource(check));
    }

    public static TimeLockCorruptionPinger jersey(TimeLockCorruptionState check) {
        return new JerseyAdapter(new TimeLockCorruptionPingerResource(check));
    }

    @Override
    public ListenableFuture<Void> corruptionDetected(AuthHeader authHeader) {
        timeLockCorruptionState.remoteHasDetectedCorruption();
        return Futures.immediateVoidFuture();
    }


    public static class JerseyAdapter implements TimeLockCorruptionPinger {
        private final TimeLockCorruptionPingerResource delegate;

        private JerseyAdapter(TimeLockCorruptionPingerResource delegate) {
            this.delegate = delegate;
        }

        @Override
        public void corruptionDetected(AuthHeader authHeader) {
            delegate.corruptionDetected(authHeader);
        }
    }
}