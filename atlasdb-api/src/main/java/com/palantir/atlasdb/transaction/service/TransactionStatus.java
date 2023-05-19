/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import com.palantir.common.annotations.ImmutablesStyles.PackageVisibleImmutablesStyle;
import java.util.Optional;
import org.immutables.value.Value;

public interface TransactionStatus {

    static TransactionStatus inProgress() {
        return ImmutableInProgress.of();
    }

    static TransactionStatus aborted() {
        return ImmutableAborted.of();
    }

    static TransactionStatus committed(long commitTimestamp) {
        return ImmutableCommitted.of(commitTimestamp);
    }

    static TransactionStatus unknown() {
        return ImmutableUnknown.of();
    }

    static Optional<Long> getCommitTimestamp(TransactionStatus status) {
        if (status instanceof Committed) {
            Committed committed = (Committed) status;
            return Optional.of(committed.commitTimestamp());
        } else {
            return Optional.empty();
        }
    }

    @Value.Immutable(singleton = true)
    @PackageVisibleImmutablesStyle
    interface InProgress extends TransactionStatus {}

    @Value.Immutable(singleton = true)
    @PackageVisibleImmutablesStyle
    interface Aborted extends TransactionStatus {}

    @Value.Immutable(builder = false)
    @PackageVisibleImmutablesStyle
    interface Committed extends TransactionStatus {

        @Value.Parameter
        long commitTimestamp();
    }

    @Value.Immutable(singleton = true)
    @PackageVisibleImmutablesStyle
    interface Unknown extends TransactionStatus {}
}
