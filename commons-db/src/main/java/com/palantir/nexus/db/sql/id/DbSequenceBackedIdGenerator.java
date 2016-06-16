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
package com.palantir.nexus.db.sql.id;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.ReentrantConnectionSupplier;
import com.palantir.nexus.db.sql.BasicSQLUtils;
import com.palantir.sql.Connections;
import com.palantir.sql.PreparedStatements;
import com.palantir.sql.ResultSets;
import com.palantir.util.jmx.OperationTimer;
import com.palantir.util.jmx.OperationTimer.TimingState;
import com.palantir.util.timer.LoggingOperationTimer;

/**
 * An ID Generator backed by a sequence in the database.
 * @author eporter
 *
 */
public class DbSequenceBackedIdGenerator implements IdGenerator {
    private static final Logger log = LoggerFactory.getLogger("com.palantir.nexus.db.SQL"); //$NON-NLS-1$
    private static final OperationTimer timer = LoggingOperationTimer.create(log);


    private static final int DUAL_N_SIZE = 65536;

    private final DBType dbType;
    private final String sequenceName;
    private final ReentrantConnectionSupplier connectionSupplier;

    public DbSequenceBackedIdGenerator(DBType dbType, String sequenceName, ReentrantConnectionSupplier connectionSupplier) {
        this.dbType = dbType;
        this.sequenceName = sequenceName;
        this.connectionSupplier = connectionSupplier;
    }

    /**
     * Because the sequence is in the database, this method will always completely fill the
     * array or throw an exception.
     * @param ids The array to completely fill with Ids from the database.
     * @return the size of the <code>ids</code>
     */
    @Override
    public int generate(final long[] ids) throws PalantirSqlException {
        if (ids.length == 0) {
            throw new IllegalArgumentException("Can't request top of 0 contiguous ids."); //$NON-NLS-1$
        }

        final String sql;
        if (dbType == DBType.POSTGRESQL) {
            sql = String.format(POSTGRESQL_TOP_MANY_ID_SQL, sequenceName.toLowerCase());
        } else {
            assert dbType == DBType.ORACLE;
            sql = String.format(ORACLE_TOP_MANY_ID_SQL, sequenceName);
        }

        BasicSQLUtils.runUninterruptably(new Callable<Void>() {
            @Override
            public Void call() throws PalantirSqlException {
                TimingState timerKey = timer.begin(sql);
                Connection c = null;
                try {
                    c = connectionSupplier.get();
                    for (int offset = 0; offset < ids.length; offset += DUAL_N_SIZE) {
                        fillWithIds(c, sql, ids, offset, Math.min(DUAL_N_SIZE, ids.length - offset));
                    }
                } finally {
                    closeQuietly(c);
                    timerKey.end();
                }
                return null;
            }
        },
                sql,
                /*
                 * This callable grabs the Connection itself, which is a direct violation of the comment on BasicSqlUtils#runUninterruptably,
                 * which claims that doing so will fail.  There does not seem to be any actual reason to change this.  So, instead of
                 * creating a connection outside the callable and passing it in this argument, we will simply pass null.
                 */
                null);
        return ids.length;
    }

    // We need to split it up on Oracle to get around the 64K limit.
    private void fillWithIds(Connection c, String sql, final long [] ids, int offset, int length)
            throws PalantirSqlException {
        PreparedStatement ps = null;
        try {
            ps = Connections.prepareStatement(c,sql);

            // The default fetch size is low (due to memory problems involving LOBs),
            // but we definitely want to load ids in as large of a batch as possible.
            PreparedStatements.setFetchSize(ps, length);
            PreparedStatements.setInt(ps, 1, length);

            ResultSet rs = PreparedStatements.executeQuery(ps);

            int stop = offset + length;
            for (int i = offset; i < stop; i++) {
                if (!ResultSets.next(rs)) {
                    String message = "Only received " + i + " of " + stop +  //$NON-NLS-1$ //$NON-NLS-2$
                        " requested ids for sequence " + sequenceName; //$NON-NLS-1$
                    assert false : message;
                    throw PalantirSqlException.create(message);
                }
                ids[i] = ResultSets.getLong(rs, 1);
            }
        } finally {
            closeQuietly(ps);
        }
    }

    private void closeQuietly(Connection c) {
        if (c != null) {
            try {
                c.close();
            } catch (SQLException e) {
                log.debug(e.getMessage(), e);
            }
        }
    }

    private void closeQuietly(PreparedStatement ps) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                log.debug(e.getMessage(), e);
            }
        }
    }

    // constants for selecting top id from a sequence
    private static final String ORACLE_TOP_MANY_ID_SQL = "SELECT %s.nextval from dual_n where rownum <= ?"; //$NON-NLS-1$
    // TODO: this only grabs one at a time. need to add a plpgsql function to really make this work
    private static final String POSTGRESQL_TOP_MANY_ID_SQL = "SELECT * FROM nextvals_%s( ? );"; //$NON-NLS-1$

}
