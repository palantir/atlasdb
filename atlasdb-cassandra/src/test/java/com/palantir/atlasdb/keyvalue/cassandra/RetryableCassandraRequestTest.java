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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.common.base.FunctionCheckedException;
import java.net.InetSocketAddress;
import org.junit.Before;
import org.junit.Test;

public class RetryableCassandraRequestTest {
    private static final int DEFAULT_PORT = 5000;
    private static final String HOSTNAME_1 = "1.0.0.0";
    private static final String HOSTNAME_2 = "2.0.0.0";
    private static final InetSocketAddress HOST_1 = InetSocketAddress.createUnresolved(HOSTNAME_1, DEFAULT_PORT);
    private static final InetSocketAddress HOST_2 = InetSocketAddress.createUnresolved(HOSTNAME_2, DEFAULT_PORT);

    private static final CassandraServer SERVER_1 = CassandraServer.of(HOST_1);
    private static final CassandraServer SERVER_2 = CassandraServer.of(HOST_2);

    private RetryableCassandraRequest<Void, RuntimeException> request;

    @Before
    public void setup() {
        request = new RetryableCassandraRequest<>(SERVER_1, noOp());
    }

    @Test
    public void totalNumberOfRetriesShouldBeZeroInitially() {
        assertNumberOfTotalAttempts(0);
    }

    @Test
    public void numberOfRetriesOnHostShouldBeZeroInitially() {
        assertNumberOfAttemptsOnHost(0, SERVER_1);
        assertNumberOfAttemptsOnHost(0, SERVER_2);
    }

    @Test
    public void shouldIncrementRetries() {
        request.triedOnHost(SERVER_1);
        assertNumberOfTotalAttempts(1);
        assertNumberOfAttemptsOnHost(1, SERVER_1);

        request.triedOnHost(SERVER_1);
        assertNumberOfTotalAttempts(2);
        assertNumberOfAttemptsOnHost(2, SERVER_1);
    }

    @Test
    public void shouldSeparateRetriesOnDifferentHosts() {
        request.triedOnHost(SERVER_1);
        assertNumberOfTotalAttempts(1);
        assertNumberOfAttemptsOnHost(1, SERVER_1);
        assertNumberOfAttemptsOnHost(0, SERVER_2);

        request.triedOnHost(SERVER_2);
        assertNumberOfTotalAttempts(2);
        assertNumberOfAttemptsOnHost(1, SERVER_1);
        assertNumberOfAttemptsOnHost(1, SERVER_2);
    }

    private void assertNumberOfTotalAttempts(int expected) {
        assertThat(request.getNumberOfAttempts()).isEqualTo(expected);
    }

    private void assertNumberOfAttemptsOnHost(int expected, CassandraServer server) {
        assertThat(request.getNumberOfAttemptsOnHost(server)).isEqualTo(expected);
    }

    private FunctionCheckedException<CassandraClient, Void, RuntimeException> noOp() {
        return new FunctionCheckedException<>() {
            @Override
            public Void apply(CassandraClient input) throws RuntimeException {
                return null;
            }

            @Override
            public String toString() {
                return "no-op";
            }
        };
    }
}
