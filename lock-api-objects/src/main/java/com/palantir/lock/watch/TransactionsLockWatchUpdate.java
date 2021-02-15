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

package com.palantir.lock.watch;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Represents a condensed view of lock watch events occurring between some known version and a set of start transaction
 * calls.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTransactionsLockWatchUpdate.class)
@JsonDeserialize(as = ImmutableTransactionsLockWatchUpdate.class)
public interface TransactionsLockWatchUpdate {
    List<LockWatchEvent> events();

    Map<Long, LockWatchVersion> startTsToSequence();

    boolean clearCache();
}
