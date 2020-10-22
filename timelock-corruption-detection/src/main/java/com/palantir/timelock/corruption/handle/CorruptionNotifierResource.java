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

package com.palantir.timelock.corruption.handle;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.conjure.java.undertow.lib.UndertowService;
import com.palantir.timelock.corruption.TimeLockCorruptionNotifier;
import com.palantir.timelock.corruption.TimeLockCorruptionNotifierEndpoints;
import com.palantir.timelock.corruption.UndertowTimeLockCorruptionNotifier;
import com.palantir.timelock.corruption.detection.RemoteCorruptionDetector;
import com.palantir.tokens.auth.AuthHeader;

public final class CorruptionNotifierResource implements UndertowTimeLockCorruptionNotifier {
    private RemoteCorruptionDetector remoteCorruptionDetector;

    private CorruptionNotifierResource(RemoteCorruptionDetector remoteCorruptionDetector) {
        this.remoteCorruptionDetector = remoteCorruptionDetector;
    }

    public static UndertowService undertow(RemoteCorruptionDetector remoteCorruptionDetector) {
        return TimeLockCorruptionNotifierEndpoints.of(new CorruptionNotifierResource(remoteCorruptionDetector));
    }

    public static TimeLockCorruptionNotifier jersey(RemoteCorruptionDetector remoteCorruptionDetector) {
        return new JerseyAdapter(new CorruptionNotifierResource(remoteCorruptionDetector));
    }

    @Override
    public ListenableFuture<Void> corruptionDetected(AuthHeader authHeader) {
        remoteCorruptionDetector.setRemoteCorruptionState();
        return Futures.immediateVoidFuture();
    }

    public static final class JerseyAdapter implements TimeLockCorruptionNotifier {
        private final CorruptionNotifierResource delegate;

        private JerseyAdapter(CorruptionNotifierResource delegate) {
            this.delegate = delegate;
        }

        @Override
        public void corruptionDetected(AuthHeader authHeader) {
            delegate.corruptionDetected(authHeader);
        }
    }
}
