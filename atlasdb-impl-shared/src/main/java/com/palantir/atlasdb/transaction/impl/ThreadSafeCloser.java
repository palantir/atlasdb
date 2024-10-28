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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.io.Closer;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class ThreadSafeCloser {
    private static final SafeLogger log = SafeLoggerFactory.get(ThreadSafeCloser.class);

    private final Closer closer = Closer.create();
    private volatile boolean isClosed = false;

    public synchronized <C extends Closeable> C register(C closeable) {
        if (isClosed) {
            try {
                closeable.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // throw new SafeIllegalStateException("cannot register new closeable if already closed");
        }
        return closer.register(closeable);
    }

    public synchronized void close() {
        if (isClosed) {
            // it's fine to call close() more than once, since it's fine to call Transaction#commit more than once.
            return;
        }
        try {
            closer.close();
            isClosed = true;
        } catch (IOException e) {
            log.warn("Error while closing resources", e);
            throw new RuntimeException(e);
        } catch (RuntimeException | Error e) {
            log.warn("Error while closing resources", e);
            throw e;
        }
    }
}
