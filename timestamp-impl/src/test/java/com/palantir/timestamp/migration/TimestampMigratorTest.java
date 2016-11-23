/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.timestamp.migration;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.palantir.timestamp.TimestampAdminService;

public class TimestampMigratorTest {
    private static final long ADMIN_SERVICE_1_TIMESTAMP = 1000L;

    private TimestampAdminService adminService1 = mock(TimestampAdminService.class);
    private TimestampAdminService adminService2 = mock(TimestampAdminService.class);

    @Before
    public void setUp() {
        when(adminService1.getUpperBoundTimestamp()).thenReturn(ADMIN_SERVICE_1_TIMESTAMP);
    }

    @Test
    public void performsMigrationWithUpperBound() {
        new TimestampMigrator(adminService1, adminService2).migrateTimestamps();
        verify(adminService1, times(1)).getUpperBoundTimestamp();
        verify(adminService2, times(1)).fastForwardTimestamp(eq(ADMIN_SERVICE_1_TIMESTAMP));
    }

    @Test
    public void invalidatesSourceService() {
        new TimestampMigrator(adminService1, adminService2).migrateTimestamps();
        verify(adminService1, times(1)).invalidateTimestamps();
    }

    @Test
    public void throwsIfSourceAndDestinationEqual() {
        assertThatThrownBy(() -> new TimestampMigrator(adminService1, adminService1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
