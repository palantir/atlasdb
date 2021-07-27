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

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.palantir.logsafe.SafeArg;
import com.palantir.timelock.corruption.TimeLockCorruptionNotifier;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalCorruptionHandler {
    private static final Logger log = LoggerFactory.getLogger(LocalCorruptionHandler.class);

    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private final List<TimeLockCorruptionNotifier> corruptionNotifiers;
    private final Retryer<Void> corruptionNotifyRetryer;

    public LocalCorruptionHandler(List<TimeLockCorruptionNotifier> corruptionNotifiers) {
        this.corruptionNotifiers = corruptionNotifiers;
        this.corruptionNotifyRetryer = new Retryer<>(
                StopStrategies.stopAfterAttempt(5),
                WaitStrategies.fixedWait(200, TimeUnit.MILLISECONDS),
                attempt -> !attempt.hasResult());
    }

    public void notifyRemoteServersOfCorruption() {
        corruptionNotifiers.forEach(this::reportCorruptionToRemote);
    }

    private void reportCorruptionToRemote(TimeLockCorruptionNotifier notifier) {
        try {
            corruptionNotifyRetryer.call(() -> {
                notifier.corruptionDetected(AUTH_HEADER);
                return null;
            });
        } catch (ExecutionException e) {
            log.info("Failed to report corruption to remote.", e);
        } catch (RetryException e) {
            log.info(
                    "Exhausted all attempts to notify remote of corruption.",
                    SafeArg.of("numberOfAttempts", e.getNumberOfFailedAttempts()),
                    e);
        }
    }
}
