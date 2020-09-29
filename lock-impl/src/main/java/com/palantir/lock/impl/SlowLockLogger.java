/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock.impl;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a logger intended for use tracking down problems arising from
 * PDS-50301 (Product hogs threads on timelock server with HTTP/2 protocol). This is currently logging the locks
 * requested in a lock request that took more than 100ms to receive a response. This will be enabled automatically
 * if you migrate to the timelock server.
 */
@SuppressFBWarnings("SLF4J_LOGGER_SHOULD_BE_PRIVATE")
public final class SlowLockLogger {
    public static final Logger logger = LoggerFactory.getLogger(SlowLockLogger.class);

    private SlowLockLogger() {
        // Logging utility class
    }
}
