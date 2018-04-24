/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.palantir.lock.client.LockRefreshingLockServiceTest;
import com.palantir.lock.impl.ClientAwareLockTest;
import com.palantir.lock.logger.LockServiceStateLoggerTest;

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
