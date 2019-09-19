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
package com.palantir.lock;

import com.palantir.lock.client.LockRefreshingLockServiceTest;
import com.palantir.lock.impl.ClientAwareLockTest;
import com.palantir.lock.logger.LockServiceStateLoggerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Runs all lock server tests.
 *
 * @author jtamer
 */
@SuiteClasses(value = {
        ClientAwareLockTest.class,
        LockServiceStateLoggerTest.class,
        LockServiceIntegrationTest.class,
        LockRefreshingLockServiceTest.class
}) @RunWith(value = Suite.class) public final class AllLockTests {
    /* Empty; the annotations above take care of everything. */
}
