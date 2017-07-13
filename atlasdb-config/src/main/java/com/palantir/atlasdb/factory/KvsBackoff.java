/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.factory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class KvsBackoff {
    private static final Logger log = LoggerFactory.getLogger(KvsBackoff.class);

    private static final double GOLDEN_RATIO = (Math.sqrt(5) + 1.0) / 2.0;
    private static final int QUICK_MAX_BACKOFF_SECONDS = 1000;

    private KvsBackoff() {
        // utility
    }

    public static void pauseForBackOff(int failureCount) {
        long timeoutInSeconds = Math.min(QUICK_MAX_BACKOFF_SECONDS, Math.round(Math.pow(GOLDEN_RATIO, failureCount)));
        try {
            log.trace("Pausing {}s before retrying", timeoutInSeconds);
            Thread.sleep(timeoutInSeconds * 1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("The thread was interrupted during backoff when creating the KVS.");
        }
    }
}
