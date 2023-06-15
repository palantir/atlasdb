/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.workflow.bank;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.util.concurrent.RateLimiter;
import com.palantir.atlasdb.workload.workflow.TableConfiguration;
import com.palantir.atlasdb.workload.workflow.WorkflowConfiguration;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableBankBalanceWorkflowConfiguration.class)
@JsonDeserialize(as = ImmutableBankBalanceWorkflowConfiguration.class)
@JsonTypeName(BankBalanceWorkflowConfiguration.TYPE)
public interface BankBalanceWorkflowConfiguration extends WorkflowConfiguration {
    String TYPE = "bank-balance";

    TableConfiguration tableConfiguration();

    @Value.Default
    default Integer numberOfAccounts() {
        return 10;
    }

    @Value.Default
    default Integer initialBalancePerAccount() {
        return 100;
    }

    @Value.Default
    default Integer transferAmount() {
        return 75;
    }

    @Value.Lazy
    default RateLimiter transactionRateLimiter() {
        return RateLimiter.create(rateLimit());
    }
}
