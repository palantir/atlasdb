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
package com.palantir.atlasdb.memory;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.google.common.base.Optional;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.timestamp.TimestampAdminService;
import com.palantir.timestamp.TimestampService;

public class InMemoryAtlasDbFactoryTest {
    private static final InMemoryAtlasDbFactory FACTORY = new InMemoryAtlasDbFactory();

    @Test
    public void canCreateKeyValueServices() {
        assertThat(FACTORY.createRawKeyValueService(new InMemoryAtlasDbConfig(), Optional.absent())).isNotNull();
    }

    @Test
    public void canCreateTimestampService() {
        KeyValueService rawKvs = FACTORY.createRawKeyValueService(new InMemoryAtlasDbConfig(), Optional.absent());
        assertThat(FACTORY.createTimestampService(rawKvs)).isNotNull();
    }

    @Test
    public void createdTimestampServiceAndTimestampAdminServiceAreLinked() {
        KeyValueService rawKvs = FACTORY.createRawKeyValueService(new InMemoryAtlasDbConfig(), Optional.absent());
        TimestampService timestampService = FACTORY.createTimestampService(rawKvs);
        TimestampAdminService timestampAdminService = FACTORY.createTimestampAdminService(rawKvs);

        long freshTimestamp = timestampService.getFreshTimestamp();
        assertThat(timestampAdminService.getUpperBoundTimestamp()).isGreaterThanOrEqualTo(freshTimestamp);

        long fastForward = freshTimestamp + 10000;
        timestampAdminService.fastForwardTimestamp(fastForward);
        assertThat(timestampService.getFreshTimestamp()).isGreaterThan(fastForward);
    }
}
