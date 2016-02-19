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
package com.palantir.atlasdb.transaction.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.TransactionServiceConfig;
import com.palantir.paxos.config.PaxosProposerConfig;

@AutoService(TransactionServiceConfig.class)
@JsonDeserialize(as = ImmutablePaxosTransactionServiceConfig.class)
@JsonSerialize(as = ImmutablePaxosTransactionServiceConfig.class)
@JsonTypeName(PaxosTransactionServiceConfig.TYPE)
@Value.Immutable
/**
 * This transaction service is a proof of concept and experimental and should not be used in production.
 */
public abstract class PaxosTransactionServiceConfig implements TransactionServiceConfig {
    public static final String TYPE = "paxos";

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Default
    public String getLogName() {
        return "transaction";
    }

    public abstract PaxosProposerConfig proposer();
}
