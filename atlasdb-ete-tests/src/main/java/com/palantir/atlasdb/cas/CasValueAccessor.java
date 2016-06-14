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
package com.palantir.atlasdb.cas;

import java.util.Objects;

import com.google.common.base.Optional;
import com.palantir.atlasdb.cas.generated.CasSchemaTableFactory;
import com.palantir.atlasdb.cas.generated.CasTable;
import com.palantir.atlasdb.transaction.api.Transaction;

public class CasValueAccessor {
    private final Transaction transaction;

    public CasValueAccessor(Transaction transaction) {
        this.transaction = transaction;
    }

    public void set(Optional<Long> value) {
        CasTable casTable = CasSchemaTableFactory.of().getCasTable(transaction);
        if(value.isPresent()) {
            casTable.putValue(CasTable.CasRow.of(0), value.get());
        } else {
            casTable.delete(CasTable.CasRow.of(0));
        }
    }

    public Optional<Long> get() {
        CasTable jepsenTable = CasSchemaTableFactory.of().getCasTable(transaction);
        return jepsenTable.getRow(CasTable.CasRow.of(0)).transform(CasTable.CasRowResult::getValue);
    }

    public boolean cas(Optional<Long> oldValue,  Optional<Long> newValue) {
        Optional<Long> existingValue = get();
        if(Objects.equals(oldValue, existingValue)) {
            set(newValue);
            return true;
        } else {
            return false;
        }
    }
}
