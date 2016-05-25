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
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowValue;
import com.palantir.db.oracle.JdbcHandler;
import com.palantir.db.oracle.JdbcHandler.ArrayHandler;

public class OracleOverflowQueryFactory extends OracleQueryFactory {
    private final OverflowMigrationState migrationState;

    public OracleOverflowQueryFactory(String tableName,
                                      OverflowMigrationState migrationState,
                                      JdbcHandler jdbcHandler) {
        super(tableName, jdbcHandler);
        this.migrationState = migrationState;
    }

    @Override
    protected String getValueSubselect(String tableAlias, boolean includeValue) {
        return includeValue ? ", " + tableAlias + ".val, " + tableAlias + ".overflow " : " ";
    }

    @Override
    public boolean hasOverflowValues() {
        return true;
    }

    @Override
    public Collection<FullQuery> getOverflowQueries(Collection<OverflowValue> overflowIds) {
        List<Object[]> oraRows = Lists.newArrayListWithCapacity(overflowIds.size());
        for (OverflowValue overflowId : overflowIds) {
            oraRows.add(new Object[] { null, null, overflowId.id });
        }
        ArrayHandler arg = jdbcHandler.createStructArray("PT_MET_CELL_TS", "PT_MET_CELL_TS_TABLE", oraRows);
        switch (migrationState) {
        case UNSTARTED:
            return ImmutableList.of(getOldOverflowQuery(arg));
        case IN_PROGRESS:
            return ImmutableList.of(getOldOverflowQuery(arg), getNewOverflowQuery(arg));
        case FINISHING: // fall through
        case FINISHED:
            return ImmutableList.of(getNewOverflowQuery(arg));
        default:
            throw new EnumConstantNotPresentException(OverflowMigrationState.class, migrationState.name());
        }
    }

    private FullQuery getOldOverflowQuery(ArrayHandler arg) {
        String query =
                " /*SQL_MET_SELECT_OVERFLOW */ " +
                " SELECT /*+ USE_NL(t o) LEADING(t o) INDEX(o pk_pt_metropolis_overflow) */ " +
                "   o.id, o.val " +
                " FROM pt_metropolis_overflow o, TABLE(CAST(? AS PT_MET_CELL_TS_TABLE)) t " +
                " WHERE t.max_ts = o.id ";
        return new FullQuery(query).withArg(arg);
    }

    private FullQuery getNewOverflowQuery(ArrayHandler arg) {
        String query =
                " /*SQL_MET_SELECT_OVERFLOW (" + tableName + ") */ " +
                " SELECT /*+ USE_NL(t o) LEADING(t o) INDEX(o pk_pt_mo_" + tableName + ") */ " +
                "   o.id, o.val " +
                " FROM pt_mo_" + tableName + " o, TABLE(CAST(? AS PT_MET_CELL_TS_TABLE)) t " +
                " WHERE t.max_ts = o.id ";
        return new FullQuery(query).withArg(arg);
    }
}
