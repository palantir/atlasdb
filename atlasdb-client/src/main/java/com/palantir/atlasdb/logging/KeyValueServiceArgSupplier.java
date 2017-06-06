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
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.UnsafeArg;

public interface KeyValueServiceArgSupplier {
    <T> Arg<T> getArgDependingOnTableReference(
            String name,
            TableReference tableReference,
            T value);

    <T> Arg<T> getArgDependingOnRowComponentName(
            String name,
            TableReference tableReference,
            NameComponentDescription nameComponentDescription,
            T value);

    <T> Arg<T> getArgDependingOnColumnName(
            String name,
            TableReference tableReference,
            NamedColumnDescription namedColumnDescription,
            T value);

    default Arg<TableReference> getArgForTableReference(String name, TableReference tableReference) {
        return getArgDependingOnTableReference(name, tableReference, tableReference);
    }

    default Arg<String> getArgForRowComponentName(
            String name, TableReference tableReference, NameComponentDescription nameComponentDescription) {
        return getArgDependingOnRowComponentName(
                name,
                tableReference,
                nameComponentDescription,
                nameComponentDescription.getComponentName());
    }

    default Arg<String> getArgForColumnLongName(
            String name, TableReference tableReference, NamedColumnDescription namedColumnDescription) {
        return getArgDependingOnColumnName(
                name,
                tableReference,
                namedColumnDescription,
                namedColumnDescription.getLongName());
    }

    KeyValueServiceArgSupplier NO_OP = new KeyValueServiceArgSupplier() {
        @Override
        public <T> Arg<T> getArgDependingOnTableReference(String name, TableReference tableReference, T value) {
            return UnsafeArg.of(name, value);
        }

        @Override
        public <T> Arg<T> getArgDependingOnRowComponentName(String name, TableReference tableReference,
                NameComponentDescription nameComponentDescription, T value) {
            return UnsafeArg.of(name, value);
        }

        @Override
        public <T> Arg<T> getArgDependingOnColumnName(String name, TableReference tableReference,
                NamedColumnDescription namedColumnDescription, T value) {
            return UnsafeArg.of(name, value);
        }
    };
}
