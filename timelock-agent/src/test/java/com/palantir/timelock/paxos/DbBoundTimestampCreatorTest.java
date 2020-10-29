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

package com.palantir.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.paxos.Client;
import org.junit.Test;

public class DbBoundTimestampCreatorTest {
    private static final Client CLIENT_1 = Client.of("tom");
    private static final Client CLIENT_2 = Client.of("jeremy");

    @Test
    public void timestampCreationParametersMaintainClientName() {
        assertThat(DbBoundTimestampCreator.getTimestampCreationParameters(CLIENT_1))
                .isEqualTo(DbTimestampCreationSetting.of(
                        AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, TimestampSeries.of(CLIENT_1.value())));

        assertThat(DbBoundTimestampCreator.getTimestampCreationParameters(CLIENT_2))
                .isEqualTo(DbTimestampCreationSetting.of(
                        AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, TimestampSeries.of(CLIENT_2.value())));
    }
}
