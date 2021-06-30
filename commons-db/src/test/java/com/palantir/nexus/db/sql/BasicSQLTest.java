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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.nexus.db.monitoring.timer.DurationSqlTimer;
import com.palantir.nexus.db.monitoring.timer.SqlTimer;
import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.junit.Test;

@SuppressWarnings("unchecked") // mocked executors
public class BasicSQLTest {
    @Test
    public void multipleInstancesOfBasicSQLCanShareExecutors() throws SQLException {
        ExecutorService selectExecutor = mock(ExecutorService.class);
        ExecutorService executeExecutor = createMockExecuteExecutor();

        BasicSQL basicSqlOne = createBasicSQL(selectExecutor, executeExecutor);
        BasicSQL basicSqlTwo = createBasicSQL(selectExecutor, executeExecutor);

        executeSqlQuery(basicSqlOne);
        executeSqlQuery(basicSqlTwo);

        verify(executeExecutor, times(2)).submit(any(Callable.class));
    }

    @Test
    public void multipleInstancesOfBasicSqlCanUseDistinctExecutors() throws SQLException {
        ExecutorService executeExecutorOne = createMockExecuteExecutor();
        ExecutorService executeExecutorTwo = createMockExecuteExecutor();

        BasicSQL basicSqlOne = createBasicSQL(mock(ExecutorService.class), executeExecutorOne);
        BasicSQL basicSqlTwo = createBasicSQL(mock(ExecutorService.class), executeExecutorTwo);

        executeSqlQuery(basicSqlOne);
        executeSqlQuery(basicSqlTwo);
        executeSqlQuery(basicSqlTwo);

        verify(executeExecutorOne, times(1)).submit(any(Callable.class));
        verify(executeExecutorTwo, times(2)).submit(any(Callable.class));
    }

    @Test
    public void testSQLException() {
        StringBuilder sb = new StringBuilder();
        byte[] bytes = new byte[] {0, 1, (byte) 0xff};
        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        // A typical atlas key is identified by row_name, column_name, ts
        BasicSQLUtils.toStringSqlArgs(sb, new Object[] {bytes, bis, 123L});
        assertThat(sb.toString())
                .isEqualTo("([B: '0001ff', java.io.ByteArrayInputStream: '0001ff', java.lang.Long: '123')\n");
    }

    private void executeSqlQuery(BasicSQL basicSql) throws SQLException {
        Connection conn = createMockConnection();
        basicSql.execute(
                conn, SQLString.getUnregisteredQuery("SELECT 1 FROM a.b;"), new Object[0], BasicSQL.AutoClose.FALSE);
    }

    private Connection createMockConnection() throws SQLException {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(any())).thenReturn(ps);
        return conn;
    }

    private ExecutorService createMockExecuteExecutor() {
        ExecutorService executeExecutor = mock(ExecutorService.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(executeExecutor.submit(any(Callable.class))).thenReturn(CompletableFuture.completedFuture(ps));
        return executeExecutor;
    }

    private BasicSQL createBasicSQL(final ExecutorService selectExecutor, final ExecutorService executeExecutor) {
        return new BasicSQL(selectExecutor, executeExecutor) {
            @Override
            protected SqlConfig getSqlConfig() {
                return new SqlConfig() {
                    @Override
                    public boolean isSqlCancellationDisabled() {
                        return false;
                    }

                    @Override
                    public SqlTimer getSqlTimer() {
                        return new DurationSqlTimer();
                    }
                };
            }
        };
    }
}
