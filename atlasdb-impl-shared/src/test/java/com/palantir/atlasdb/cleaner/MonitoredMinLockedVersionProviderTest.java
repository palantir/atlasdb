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

package com.palantir.atlasdb.cleaner;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;

import org.assertj.core.data.Offset;
import org.junit.Test;

import com.palantir.lock.LockClient;

import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public class MonitoredMinLockedVersionProviderTest {


    private static class TestMonitoredMinLockedVersionProvider extends MonitoredMinLockedVersionProvider {
        TestMonitoredMinLockedVersionProvider() {
            super(null, LockClient.of("Test client"));
        }

        @Override
        protected TimeUnit getTimeUnit() {
            return TimeUnit.SECONDS;
        }

        @Override
        Long getMinLockedVersion() {
            return 0L;
        }
    }

    @Test
    public void testSetupMonitoring() throws Exception {
        TestLogger testLogger = TestLoggerFactory.getTestLogger(MonitoredMinLockedVersionProvider.class);
        testLogger.setEnabledLevelsForAllThreads(Level.WARN);

        MonitoredMinLockedVersionProvider versionProvider = new TestMonitoredMinLockedVersionProvider();
        int numSecondsToWait = 5;

        // 500ms to make other thread perform all 5 operations
        Thread.sleep(numSecondsToWait * versionProvider.getTimeUnit().toMillis(1) + 500);

        // As most action happens in a different thread, to avoid flickery tests, assume +-1 error margin
        assertThat(testLogger.getAllLoggingEvents().size()).isCloseTo(numSecondsToWait, Offset.offset(1));
    }
}