/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.nexus.db;

import org.apache.commons.lang3.Validate;

public class SqlClause {

    private final String key;
    private final String clause;

    public SqlClause(String key, String clause) {
        Validate.notNull(key, "key should not be null");
        Validate.notNull(clause, "clause should not be null");
        this.key = key;
        this.clause = clause;
    }

    public String getClause() {
        return clause;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return key + ": " + clause; //$NON-NLS-1$
    }
}
