/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.proxy;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;

import com.palantir.logsafe.exceptions.SafeRuntimeException;

public class ResilientDatabaseConnectionProxyTest {
    private Connection mockConnection = mock(Connection.class);
    private Supplier<Connection> connectionSupplier = mock(Supplier.class);

    @Before
    public void setupMock() throws SQLException {
        AtomicBoolean connectionClosed = new AtomicBoolean(false);
        when(mockConnection.isClosed()).thenAnswer(invocation -> connectionClosed.get());
        doAnswer(invocation -> {
            connectionClosed.set(true);
            return null;
        }).when(mockConnection).close();
        when(connectionSupplier.get()).thenReturn(mockConnection);
    }

    @Test
    public void repeatedlyClosingDoesNotCreateNewConnections() throws SQLException {
        Connection connection = ResilientDatabaseConnectionProxy.newProxyInstance(connectionSupplier);

        int numCloses = 5;
        for (int i = 0; i < numCloses; i++) {
            connection.close();
        }

        verify(connectionSupplier, times(1)).get();
        verify(mockConnection, times(5)).close();
    }

    @Test
    public void tryToEstablishNewConnectionIfClosedAndRunInvocationOnThatConnectionOnly() throws SQLException {
        mockConnection.close();
        Connection openConnection = mock(Connection.class);
        when(openConnection.isClosed()).thenReturn(false);
        when(connectionSupplier.get()).thenReturn(mockConnection, mockConnection, openConnection);
        Connection connection = ResilientDatabaseConnectionProxy.newProxyInstance(connectionSupplier);

        assertThatCode(connection::commit).doesNotThrowAnyException();
        verify(openConnection, times(1)).commit();
        verify(mockConnection, never()).commit();
    }

    @Test
    public void eventuallyThrowIfUnableToGetAnOpenConnection() throws SQLException {
        mockConnection.close();
        Connection connection = ResilientDatabaseConnectionProxy.newProxyInstance(connectionSupplier);

        assertThatThrownBy(connection::commit).isInstanceOf(SafeRuntimeException.class);
        verify(connectionSupplier, atLeast(10)).get();
        verify(mockConnection, never()).commit();
    }

    @Test
    public void propagateSqlException() throws SQLException {
        SQLException exception = new SQLException("Bad");
        doThrow(exception).when(mockConnection).commit();
        Connection connection = ResilientDatabaseConnectionProxy.newProxyInstance(connectionSupplier);

        assertThatThrownBy(connection::commit).isEqualTo(exception);
    }
}
