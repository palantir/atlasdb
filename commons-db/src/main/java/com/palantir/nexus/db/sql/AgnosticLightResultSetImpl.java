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
package com.palantir.nexus.db.sql;

import com.palantir.common.base.Throwables;
import com.palantir.common.visitor.Visitor;
import com.palantir.exception.PalantirInterruptedException;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import com.palantir.nexus.db.sql.BasicSQLString.FinalSQLString;
import com.palantir.sql.ResultSets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This result set only loads one row at a time, and thus provides a
 * low-overhead solution for large queries.  Read the comments about the iterator before
 * using it, because there are non-obvious pitfalls.
 * @author dcohen
 *
 */
class AgnosticLightResultSetImpl implements AgnosticLightResultSet {
    private static final Logger log = LoggerFactory.getLogger(AgnosticLightResultSetImpl.class);
    private static final Logger sqlExceptionlog =
            LoggerFactory.getLogger("sqlException." + AgnosticLightResultSetImpl.class.getName());

    private final ResultSet results;
    private final DBType dbType;
    private final Map<String, Integer> columnMap;
    private final PreparedStatement stmt;
    private final AgnosticLightResultRow
            singletonResultRow; // This is here and not in the iterator so we can throw a SQLException when constructing
    // it
    private volatile boolean hasBeenClosed = false;
    private SqlTimer.Handle timerKey;
    private final String timingModule;
    private final FinalSQLString sqlString;
    private int maxFetchSize = DEFAULT_MAX_FETCH_SIZE;
    private final SqlTimer sqlTimerFactory;

    public AgnosticLightResultSetImpl(
            ResultSet rs,
            DBType type,
            ResultSetMetaData meta,
            PreparedStatement s,
            String timingModule,
            FinalSQLString sqlString,
            SqlTimer sqlTimerFactory)
            throws PalantirSqlException {
        results = rs;
        dbType = type;
        stmt = s;

        columnMap = ResultSets.buildJdbcColumnMap(meta, type);
        singletonResultRow = new AgnosticLightResultRowImpl(dbType, columnMap, results);

        this.timingModule = timingModule;
        this.sqlString = sqlString;
        this.sqlTimerFactory = sqlTimerFactory;
    }

    @Override
    public void close() {
        if (hasBeenClosed) {
            return;
        }
        timerKey.stop();
        try {
            stmt.close();
            results.close();
            hasBeenClosed = true;
            log.debug("Closed {}", this);
        } catch (SQLException sqlex) {
            log.error("Caught SQLException", sqlex); // $NON-NLS-1$
        }
    }

    @Override
    public void setFetchSize(int fetchSize) {
        maxFetchSize = fetchSize;
        try {
            results.setFetchSize(fetchSize);
        } catch (SQLException e) {
            sqlExceptionlog.info("Caught SQLException", e); // $NON-NLS-1$
            log.error("Caught SQLException", e); // $NON-NLS-1$
            Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    /**
     * @see AgnosticIterator for caveats on use.
     * Note this expects to be called by visitAndClose()! If not, the timer will never get
     * initialized and close() will throw an exception.
     */
    @Override
    public Iterator<AgnosticLightResultRow> iterator() {
        this.timerKey = sqlTimerFactory.start(timingModule, sqlString.getKey(), sqlString.getQuery());
        return new AgnosticIterator();
    }

    protected static final int INITIAL_FETCH_SIZE = 10;

    // NOTE: the following was decreased from its original setting of 1000,
    // which seemed to cause the server to churn through huge amounts of memory
    // quickly on certain workloads. Documentation suggests that values over 50
    // are not useful.
    protected static final int DEFAULT_MAX_FETCH_SIZE = 50;

    /**
     * Geometrically increases the fetch size, so that we don't have to GC a lot
     * memory for small (or empty) result sets, but we still do reasonable
     * batching for large result sets.
     *
     * <strong> Note: calling hasNext() can invalidate the previous AgnosticLightResultRow
     * that was fetched from next().  This means that <code> item = it.next(); isDone = it.hasNext(); foo(item); </code>
     * may cause an error! </strong>  In more detail: the first time hasNext() is called after each call to next(), it advances
     * the database cursor, causing any old AgnosticLightResultRow objects to now refer to the next row in the database.
     */
    private final class AgnosticIterator implements Iterator<AgnosticLightResultRow> {
        private int numRowsFetchedSinceLastChange = 0;
        private boolean hasReadRow = true; // $NON-NLS-1$  // Effectively "has already read row # -1"
        private boolean hasNext = false;

        private AgnosticIterator() {
            try {
                results.setFetchSize(INITIAL_FETCH_SIZE);
            } catch (SQLException e) {
                sqlExceptionlog.info("Caught SQLException", e);
                log.error("Caught SQLException", e); // $NON-NLS-1$
                Throwables.rewrapAndThrowUncheckedException(e);
            }
        }

        @Override
        public boolean hasNext() {
            if (hasReadRow) {
                advanceRow();
            }
            return hasNext;
        }

        private void advanceRow() {
            if (Thread.currentThread().isInterrupted()) {
                throw new PalantirInterruptedException("Interrupted while iterating through results."); // $NON-NLS-1$
            }
            try {
                numRowsFetchedSinceLastChange++;
                if (numRowsFetchedSinceLastChange >= results.getFetchSize()) {
                    results.setFetchSize(Math.min(results.getFetchSize() * 2, maxFetchSize));
                    numRowsFetchedSinceLastChange = 0;
                }
            } catch (SQLException e) {
                sqlExceptionlog.info("Caught SQLException", e);
                log.error("Caught SQLException", e); // $NON-NLS-1$
                Throwables.rewrapAndThrowUncheckedException(e);
            }

            try {
                hasNext = results.next();
            } catch (SQLException sqlex) {
                log.error("Caught SQLException", sqlex); // $NON-NLS-1$
                Throwables.rewrapAndThrowUncheckedException(sqlex);
            }
            hasReadRow = false;
        }

        @Override
        public AgnosticLightResultRow next() {
            if (hasReadRow) {
                advanceRow();
            }
            hasReadRow = true;
            return singletonResultRow;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove results from SQL cursor"); // $NON-NLS-1$
        }
    }

    @Override
    public void visitAndClose(Visitor<? super AgnosticLightResultRow> visitor) throws PalantirSqlException {
        try {
            for (AgnosticLightResultRow row : this) {
                visitor.visit(row);
            }
        } finally {
            try {
                close();
            } catch (RuntimeException e) {
                log.warn("Error while closing visitor", e);
            }
        }
    }
}
