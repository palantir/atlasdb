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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.palantir.common.concurrent.PTExecutors;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class ConnectionWithCallbackTest {
    public final AtomicInteger counter = new AtomicInteger();

    public final Connection connection = mock(Connection.class);

    @Test
    public void callbackRunsOnlyOnceWhenConnectionIsClosed() {
        Connection connectionWithCallback = ConnectionWithCallback.wrap(connection, counter::incrementAndGet);

        assertThat(counter).hasValue(0);

        ExecutorService executor = PTExecutors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(1);
        List<Future<Void>> results = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            results.add(executor.submit(() -> {
                try {
                    latch.await();
                    connectionWithCallback.close();
                    return null;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        latch.countDown();

        try {
            for (Future<Void> singleFuture : results) {
                assertThatCode(singleFuture::get).doesNotThrowAnyException();
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(counter).hasValue(1);
    }

    @Test
    public void callbackRunsIfCloseThrows() throws SQLException {
        SQLException exception = new SQLException();
        doThrow(exception).when(connection).close();
        Connection connectionWithCallback = ConnectionWithCallback.wrap(connection, counter::incrementAndGet);

        assertThatThrownBy(connectionWithCallback::close).isEqualTo(exception);
        assertThat(counter).hasValue(1);
    }

    @Test
    public void callbackDoesNotRunOnOtherMethods() throws SQLException {
        Connection connectionWithCallback = ConnectionWithCallback.wrap(connection, counter::incrementAndGet);
        connectionWithCallback.commit();
        assertThat(counter).hasValue(0);
    }
}
