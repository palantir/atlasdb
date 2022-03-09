/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.nexus.db.pool;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.sql.Connection;
import java.sql.SQLException;
import org.junit.Test;

public class HikariClientPoolConnectionManagerViewTest {
    private final HikariCPConnectionManager sharedManager = mock(HikariCPConnectionManager.class);

    @Test
    public void cannotTakeOutMoreThanLimitConnections() {
        ConnectionManager connectionManager = new HikariClientPoolConnectionManagerView(sharedManager, 10, 0);

        for (int i = 0; i < 10; i++) {
            assertThatCode(connectionManager::getConnection).doesNotThrowAnyException();
        }

        assertGetConnectionTimesOut(connectionManager);
        assertGetConnectionTimesOut(connectionManager);
    }

    @Test
    public void unsuccessfulCallsToDelegateDoNotCount() throws SQLException {
        SQLException sqlException = new SQLException();
        doReturn(null).doThrow(sqlException).doReturn(null).when(sharedManager).getConnection();
        ConnectionManager connectionManager = new HikariClientPoolConnectionManagerView(sharedManager, 2, 0);

        assertThatCode(connectionManager::getConnection).doesNotThrowAnyException();
        assertThatThrownBy(connectionManager::getConnection).isEqualTo(sqlException);
        assertThatCode(connectionManager::getConnection).doesNotThrowAnyException();
        assertGetConnectionTimesOut(connectionManager);
    }

    @Test
    public void closingReturnedConnectionAllowsNewConnectionsToBeBorrowed() throws SQLException {
        when(sharedManager.getConnection()).thenReturn(mock(Connection.class));
        ConnectionManager connectionManager = new HikariClientPoolConnectionManagerView(sharedManager, 1, 0);

        try (Connection connection = connectionManager.getConnectionUnchecked()) {
            assertGetConnectionTimesOut(connectionManager);
        } catch (SQLException throwables) {
            fail("This was unexpected");
        }
        assertThatCode(connectionManager::getConnection).doesNotThrowAnyException();
    }

    @Test
    public void multipleViewsAreIndependent() throws SQLException {
        when(sharedManager.getConnection()).thenReturn(mock(Connection.class));
        ConnectionManager view1 = new HikariClientPoolConnectionManagerView(sharedManager, 1, 0);
        ConnectionManager view2 = new HikariClientPoolConnectionManagerView(sharedManager, 1, 0);

        assertThatCode(view1::getConnection).doesNotThrowAnyException();
        assertGetConnectionTimesOut(view1);
        Connection connection = view2.getConnection();
        assertGetConnectionTimesOut(view2);
        connection.close();
        assertGetConnectionTimesOut(view1);
        assertThatCode(view2::getConnection).doesNotThrowAnyException();
    }

    private static void assertGetConnectionTimesOut(ConnectionManager connectionManager) {
        assertThatThrownBy(connectionManager::getConnection)
                .isInstanceOf(SafeRuntimeException.class)
                .hasMessageContaining("timed out waiting to acquire connection");
    }
}
