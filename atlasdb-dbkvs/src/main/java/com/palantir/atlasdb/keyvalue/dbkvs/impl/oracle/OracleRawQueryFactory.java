/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.util.Collection;

import com.palantir.atlasdb.keyvalue.dbkvs.OracleKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowValue;

public class OracleRawQueryFactory extends OracleQueryFactory {

    public OracleRawQueryFactory(String tableName, OracleKeyValueServiceConfig config) {
        super(tableName, config);
    }

    @Override
    String getValueSubselect(String tableAlias, boolean includeValue) {
        return includeValue ? ", " + tableAlias + ".val " : " ";
    }

    @Override
    public boolean hasOverflowValues() {
        return false;
    }

    @Override
    public Collection<FullQuery> getOverflowQueries(Collection<OverflowValue> overflowIds) {
        throw new IllegalStateException("raw tables don't have overflow fields");
    }
}
