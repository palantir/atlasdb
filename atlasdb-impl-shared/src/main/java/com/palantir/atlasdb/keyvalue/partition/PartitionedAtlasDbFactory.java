/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.partition;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.TimestampServiceConfig;
import com.palantir.atlasdb.spi.TransactionServiceConfig;
import com.palantir.atlasdb.transaction.config.PaxosTransactionServiceConfig;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class PartitionedAtlasDbFactory implements AtlasDbFactory {

    @Override
    public String getType() {
        return "partitioned";
    }

    @Override
    public PartitionedKeyValueService createRawKeyValueService(KeyValueServiceConfig config) {
        return PartitionedKeyValueService.create((PartitionedKeyValueConfiguration) config);
    }

    @Override
    public TimestampService createTimestampService(Optional<TimestampServiceConfig> config,
                                                   KeyValueService rawKvs) {
        return null;
    }

    @Override
    public TransactionService createTransactionService(Optional<TransactionServiceConfig> config, KeyValueService rawKvs) {
        Preconditions.checkArgument(config.isPresent() && config.get() instanceof PaxosTransactionServiceConfig);
        return TransactionServices.createTransactionService(config, rawKvs);
    }

}
