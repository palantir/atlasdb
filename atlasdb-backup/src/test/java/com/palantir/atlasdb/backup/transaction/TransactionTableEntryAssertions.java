/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.backup.transaction;

import static org.assertj.core.api.Assertions.fail;

import com.palantir.atlasdb.pue.PutUnlessExistsValue;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class TransactionTableEntryAssertions {
    private TransactionTableEntryAssertions() {
        // no
    }

    public static void aborted(TransactionTableEntry entry, Consumer<Long> assertions) {
        TransactionTableEntries.caseOf(entry)
                .explicitlyAborted(startTs -> {
                    assertions.accept(startTs);
                    return null;
                })
                .otherwise(() -> fail("UnexpectedEntryType"));
    }

    public static void legacy(TransactionTableEntry entry, BiConsumer<Long, Long> assertions) {
        TransactionTableEntries.caseOf(entry)
                .committedLegacy((startTs, commitTs) -> {
                    assertions.accept(startTs, commitTs);
                    return null;
                })
                .otherwise(() -> fail("UnexpectedEntryType"));
    }

    public static void twoPhase(TransactionTableEntry entry, BiConsumer<Long, PutUnlessExistsValue<Long>> assertions) {
        TransactionTableEntries.caseOf(entry)
                .committedTwoPhase((startTs, commitTs) -> {
                    assertions.accept(startTs, commitTs);
                    return null;
                })
                .otherwise(() -> fail("UnexpectedEntryType"));
    }
}
