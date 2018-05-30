/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;

import java.util.concurrent.ThreadLocalRandom;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

@Value.Immutable
@JsonSerialize(as = ImmutableSamplingTransactionCondition.class)
@JsonDeserialize(as = ImmutableSamplingTransactionCondition.class)
@JsonTypeName(SamplingTransactionCondition.TYPE)
public interface SamplingTransactionCondition extends TransactionCondition {
    String TYPE = "sampling";

    SamplingTransactionCondition NEVER_SAMPLE = SamplingTransactionCondition.of(0d);

    @Value.Default
    default double rate() {
        return 0d;
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(0d <= rate() && rate() <= 1d, "capture rate must be between 0 and 1 inclusive");
    }

    @Override
    default boolean test(Transaction transaction) {
        return ThreadLocalRandom.current().nextDouble() < rate();
    }

    static ImmutableSamplingTransactionCondition.Builder builder() {
        return ImmutableSamplingTransactionCondition.builder();
    }

    static SamplingTransactionCondition of(double rate) {
        return SamplingTransactionCondition.builder().rate(rate).build();
    }
}
