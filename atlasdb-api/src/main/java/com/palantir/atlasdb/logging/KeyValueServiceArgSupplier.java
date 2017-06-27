/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.logging;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.UnsafeArg;

public interface KeyValueServiceArgSupplier {
    KeyValueServiceArgSupplier ALL_UNSAFE = new KeyValueServiceArgSupplier() {
        @Override
        public Arg<TableReference> getTableReferenceArg(String argName, TableReference tableReference) {
            return UnsafeArg.of(argName, tableReference);
        }

        @Override
        public Arg<String> getRowComponentNameArg(String argName, TableReference tableReference,
                String rowComponentName) {
            return UnsafeArg.of(argName, rowComponentName);
        }

        @Override
        public Arg<String> getColumnNameArg(String argName, TableReference tableReference, String columnName) {
            return UnsafeArg.of(argName, columnName);
        }
    };

    Arg<TableReference> getTableReferenceArg(String argName, TableReference tableReference);

    Arg<String> getRowComponentNameArg(String argName, TableReference tableReference, String rowComponentName);

    Arg<String> getColumnNameArg(String argName, TableReference tableReference, String columnName);
}
