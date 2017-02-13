/**
 * Copyright 2016 Palantir Technologies
 * ​
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * ​
 * http://opensource.org/licenses/BSD-3-Clause
 * ​
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.spi;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.timestamp.TimestampService;
import com.palantir.timestamp.TimestampStoreInvalidator;

public interface AtlasDbFactory {
    String getType();

    KeyValueService createRawKeyValueService(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig);

    TimestampService createTimestampService(KeyValueService rawKvs);

    default TimestampStoreInvalidator createTimestampStoreInvalidator(KeyValueService rawKvs) {
        return () -> {
            throw new UnsupportedOperationException("AtlasDB doesn't support automated migration for KVS type: "
                    + getType());
        };
    }
}
