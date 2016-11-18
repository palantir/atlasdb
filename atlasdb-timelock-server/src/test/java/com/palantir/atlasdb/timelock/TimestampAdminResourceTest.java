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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.ws.rs.NotFoundException;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.timestamp.TimestampAdminService;

public class TimestampAdminResourceTest {
    private static final String EXISTING_CLIENT_1 = "client-1";
    private static final String EXISTING_CLIENT_2 = "client-2";
    private static final String NON_EXISTING_CLIENT = "non-existing-client";

    private static final TimestampAdminService SERVICE_1 = mock(TimestampAdminService.class);
    private static final TimestampAdminService SERVICE_2 = mock(TimestampAdminService.class);
    private static final TimestampAdminResource RESOURCE = new TimestampAdminResource(
            ImmutableMap.of(EXISTING_CLIENT_1, SERVICE_1,
                    EXISTING_CLIENT_2, SERVICE_2));


    @Test
    public void canGetExistentTimestampAdminService() {
        assertThat(RESOURCE.getTimestampAdminService(EXISTING_CLIENT_1)).isEqualTo(SERVICE_1);
        assertThat(RESOURCE.getTimestampAdminService(EXISTING_CLIENT_2)).isEqualTo(SERVICE_2);
    }

    @Test
    public void respectsDifferentClientsWhenGettingTimestampAdminServices() {
        assertThat(RESOURCE.getTimestampAdminService(EXISTING_CLIENT_1)).isNotEqualTo(SERVICE_2);
        assertThat(RESOURCE.getTimestampAdminService(EXISTING_CLIENT_2)).isNotEqualTo(SERVICE_1);
    }

    @Test(expected = NotFoundException.class)
    public void throwsOnNonexistentTimestampAdminService() {
        RESOURCE.getTimestampAdminService(NON_EXISTING_CLIENT);
    }
}
