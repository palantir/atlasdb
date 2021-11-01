/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.logging;

import com.palantir.atlasdb.keyvalue.api.TableReference;

interface KeyValueServiceLogArbitrator {
    KeyValueServiceLogArbitrator ALL_UNSAFE = new KeyValueServiceLogArbitrator() {
        @Override
        public boolean isTableReferenceSafe(TableReference _tableReference) {
            return false;
        }

        @Override
        public boolean isInternalTableReferenceSafe(String _internalTableReference) {
            return false;
        }

        @Override
        public boolean isRowComponentNameSafe(TableReference _tableReference, String _rowComponentName) {
            return false;
        }

        @Override
        public boolean isColumnNameSafe(TableReference _tableReference, String _longNameForColumn) {
            return false;
        }
    };

    KeyValueServiceLogArbitrator ALL_SAFE = new KeyValueServiceLogArbitrator() {
        @Override
        public boolean isTableReferenceSafe(TableReference _tableReference) {
            return true;
        }

        @Override
        public boolean isInternalTableReferenceSafe(String _internalTableReference) {
            return true;
        }

        @Override
        public boolean isRowComponentNameSafe(TableReference _tableReference, String _rowComponentName) {
            return true;
        }

        @Override
        public boolean isColumnNameSafe(TableReference _tableReference, String _longNameForColumn) {
            return true;
        }
    };

    boolean isTableReferenceSafe(TableReference tableReference);

    boolean isInternalTableReferenceSafe(String internalTableReference);

    boolean isRowComponentNameSafe(TableReference tableReference, String rowComponentName);

    boolean isColumnNameSafe(TableReference tableReference, String longNameForColumn);
}
