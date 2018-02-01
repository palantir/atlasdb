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

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.HeldLocksToken;

@Value.Immutable
@JsonSerialize(as = ImmutableConjunctiveTransactionTaskCondition.class)
@JsonDeserialize(as = ImmutableConjunctiveTransactionTaskCondition.class)
@JsonTypeName(ConjunctiveTransactionTaskCondition.TYPE)
public interface ConjunctiveTransactionTaskCondition extends TransactionTaskCondition {
    String TYPE = "conjunctive";

    TransactionTaskCondition first();
    TransactionTaskCondition second();

    @Override
    default <T, E extends Exception> boolean test(Transaction transaction, TransactionTask<T, E> task) {
        return first().test(transaction, task) && second().test(transaction, task);
    }

    @Override
    default <T, E extends Exception> boolean test(Transaction transaction, Iterable<HeldLocksToken> locks,
            LockAwareTransactionTask<T, E> task) {
        return first().test(transaction, locks, task) && second().test(transaction, locks, task);
    }

    @Override
    default <T, C extends PreCommitCondition, E extends Exception> boolean test(Transaction transaction,
            C preCommitCondition, ConditionAwareTransactionTask<T, C, E> task) {
        return first().test(transaction, preCommitCondition, task)
                && second().test(transaction, preCommitCondition, task);
    }

    static ConjunctiveTransactionTaskCondition of(TransactionTaskCondition first, TransactionTaskCondition second) {
        return builder().first(first).second(second).build();
    }

    static ImmutableConjunctiveTransactionTaskCondition.Builder builder() {
        return ImmutableConjunctiveTransactionTaskCondition.builder();
    }
}
