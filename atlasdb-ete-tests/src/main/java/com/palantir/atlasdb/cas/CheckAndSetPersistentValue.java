/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cas;

import java.util.Objects;
import java.util.Optional;

import com.palantir.atlasdb.cas.generated.CheckAndSetSchemaTableFactory;
import com.palantir.atlasdb.cas.generated.CheckAndSetTable;
import com.palantir.atlasdb.transaction.api.Transaction;

public class CheckAndSetPersistentValue {
    private static final CheckAndSetTable.CheckAndSetRow CHECK_AND_SET_ROW = CheckAndSetTable.CheckAndSetRow.of(0);

    private final Transaction transaction;

    public CheckAndSetPersistentValue(Transaction transaction) {
        this.transaction = transaction;
    }

    public void set(Optional<Long> value) {
        CheckAndSetTable checkAndSetTable = CheckAndSetSchemaTableFactory.of().getCheckAndSetTable(transaction);
        if (value.isPresent()) {
            checkAndSetTable.putValue(CHECK_AND_SET_ROW, value.get());
        } else {
            checkAndSetTable.delete(CHECK_AND_SET_ROW);
        }
    }

    public Optional<Long> get() {
        CheckAndSetTable checkAndSetTable = CheckAndSetSchemaTableFactory.of().getCheckAndSetTable(transaction);
        return checkAndSetTable.getRow(CHECK_AND_SET_ROW).map(CheckAndSetTable.CheckAndSetRowResult::getValue);
    }

    public boolean checkAndSet(Optional<Long> oldValue, Optional<Long> newValue) {
        Optional<Long> existingValue = get();
        if (Objects.equals(oldValue, existingValue)) {
            set(newValue);
            return true;
        } else {
            return false;
        }
    }
}
