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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleDdlConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.OracleTableNameGetter;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionSupplier;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowMigrationState;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowValueLoader;
import com.palantir.common.base.ClosableIterator;
import com.palantir.common.base.ClosableIterators;
import com.palantir.common.exception.TableMappingNotFoundException;
import com.palantir.db.oracle.JdbcHandler.ArrayHandler;
import com.palantir.nexus.db.sql.AgnosticLightResultRow;
import com.palantir.nexus.db.sql.AgnosticLightResultSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class OracleOverflowValueLoader implements OverflowValueLoader {

    private final OracleDdlConfig config;
    private final OracleTableNameGetter tableNameGetter;

    public OracleOverflowValueLoader(OracleDdlConfig config,
                                     OracleTableNameGetter tableNameGetter) {
        this.config = config;
        this.tableNameGetter = tableNameGetter;
    }

    @Override
    public Map<Long, byte[]> loadOverflowValues(ConnectionSupplier conns,
                                                TableReference tableRef,
                                                Collection<Long> overflowIds) {
        if (overflowIds.isEmpty()) {
            return Collections.emptyMap();
        } else {
            Map<Long, byte[]> ret = Maps.newHashMapWithExpectedSize(overflowIds.size());
            for (FullQuery query : getOverflowQueries(conns, tableRef, overflowIds)) {
                try (ClosableIterator<AgnosticLightResultRow> overflowIter = select(conns, query)) {
                    while (overflowIter.hasNext()) {
                        AgnosticLightResultRow row = overflowIter.next();
                        // QA-94468 LONG RAW typed columns ("val" in this case) must be retrieved first from the result
                        // set. See https://docs.oracle.com/cd/B19306_01/java.102/b14355/jstreams.htm#i1007581
                        byte[] val = row.getBytes("val");
                        long id = row.getLong("id");
                        ret.put(id, val);
                    }
                }
            }
            return ret;
        }
    }

    private ClosableIterator<AgnosticLightResultRow> select(ConnectionSupplier conns, FullQuery query) {
        AgnosticLightResultSet results = conns.get().selectLightResultSetUnregisteredQuery(
                query.getQuery(), query.getArgs());
        return ClosableIterators.wrap(results.iterator(), results);
    }

    private List<FullQuery> getOverflowQueries(ConnectionSupplier conns,
                                               TableReference tableRef,
                                               Collection<Long> overflowIds) {
        List<Object[]> oraRows = Lists.newArrayListWithCapacity(overflowIds.size());
        for (Long overflowId : overflowIds) {
            oraRows.add(new Object[] { null, null, overflowId });
        }
        ArrayHandler arg = config.jdbcHandler().createStructArray(
                structArrayPrefix() + "CELL_TS",
                structArrayPrefix() + "CELL_TS_TABLE", oraRows);
        switch (config.overflowMigrationState()) {
            case UNSTARTED:
                return ImmutableList.of(getOldOverflowQuery(arg));
            case IN_PROGRESS:
                return ImmutableList.of(getOldOverflowQuery(arg), getNewOverflowQuery(conns, tableRef, arg));
            case FINISHING:
            case FINISHED:
                return ImmutableList.of(getNewOverflowQuery(conns, tableRef, arg));
            default:
                throw new EnumConstantNotPresentException(
                        OverflowMigrationState.class, config.overflowMigrationState().name());
        }
    }

    private FullQuery getOldOverflowQuery(ArrayHandler arg) {
        String query = " /* SELECT_OVERFLOW */ "
                + " SELECT"
                + "   /*+ USE_NL(t o) LEADING(t o) INDEX(o "
                + PrimaryKeyConstraintNames.get(config.singleOverflowTable()) + ") */ "
                + "   o.id, o.val "
                + " FROM " + config.singleOverflowTable() + " o, "
                + "   TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE t.max_ts = o.id ";
        return new FullQuery(query).withArg(arg);
    }

    private FullQuery getNewOverflowQuery(ConnectionSupplier conns,
                                          TableReference tableRef,
                                          ArrayHandler arg) {
        String overflowTableName = getOverflowTableName(conns, tableRef);
        String query = " /* SELECT_OVERFLOW (" + overflowTableName + ") */ "
                + " SELECT"
                + "   /*+ USE_NL(t o) LEADING(t o) INDEX(o "
                + PrimaryKeyConstraintNames.get(overflowTableName) + ") */ "
                + "   o.id, o.val "
                + " FROM " + overflowTableName + " o,"
                + "   TABLE(CAST(? AS " + structArrayPrefix() + "CELL_TS_TABLE)) t "
                + " WHERE t.max_ts = o.id ";
        return new FullQuery(query).withArg(arg);
    }

    private String getOverflowTableName(ConnectionSupplier connectionSupplier, TableReference tableRef) {
        try {
            return tableNameGetter.getInternalShortOverflowTableName(connectionSupplier, tableRef);
        } catch (TableMappingNotFoundException e) {
            throw Throwables.propagate(e);
        }
    }

    private String structArrayPrefix() {
        return config.tablePrefix().toUpperCase();
    }
}
