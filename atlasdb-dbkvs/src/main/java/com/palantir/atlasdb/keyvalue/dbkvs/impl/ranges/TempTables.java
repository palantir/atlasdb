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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.AgnosticResultRow;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SQLString;
import com.palantir.nexus.db.sql.SQLString.RegisteredSQLString;
import com.palantir.nexus.db.sql.SqlConnection;

public class TempTables {
    private static final Logger log = LoggerFactory.getLogger(TempTables.class);

    private TempTables() { /* empty */ }

    private static void executeInstrumentedSql(SqlConnection c, RegisteredSQLString query, String identifier, Stopwatch stopwatch) {
        c.execute(query);
        log.info(identifier + " took " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms.");
        stopwatch.reset().start();
    }

    static void truncateRowTable(SqlConnection sql) throws PalantirSqlException {
        Stopwatch stopwatch = Stopwatch.createStarted();
        executeInstrumentedSql(sql, SQL_MET_TEMP_ROW_TRUNCATE, "truncateRowTable", stopwatch);
    }

    static TreeMultimap<Integer, byte[]> getRowsForBatches(SqlConnection c) {
        AgnosticResultSet results = c.selectResultSet(SQL_MET_ROW_TEMP_GET_ALL);
        TreeMultimap<Integer, byte[]> ret = TreeMultimap.create(
                Ordering.natural(),
                UnsignedBytes.lexicographicalComparator());
        for (AgnosticResultRow row : results.rows()) {
            @SuppressWarnings("deprecation")
            byte[] rowName = row.getBytes("row_name");
            int batchNum = row.getInteger("batch_num");
            if (rowName != null) {
                ret.put(batchNum, rowName);
            }
        }
        return ret;
    }

    private static final RegisteredSQLString SQL_MET_ROW_TEMP_GET_ALL = SQLString.registerQuery(
            "SQL_MET_ROW_TEMP_GET_ALL", " select * from pt_metropolis_row_temp");

    private static final RegisteredSQLString SQL_MET_TEMP_ROW_TRUNCATE = SQLString.registerQuery(
            "SQL_MET_TEMP_ROW_TRUNCATE", "TRUNCATE TABLE pt_metropolis_row_temp");
}
