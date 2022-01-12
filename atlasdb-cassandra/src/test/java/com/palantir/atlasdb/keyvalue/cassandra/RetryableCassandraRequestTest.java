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

    private RetryableCassandraRequest<Void, RuntimeException> request;

    @Before
    public void setup() {
        request = new RetryableCassandraRequest<>(HOST_1, noOp());
    }

    @Test
    public void totalNumberOfRetriesShouldBeZeroInitially() {
        assertNumberOfTotalAttempts(0);
    }

    @Test
    public void numberOfRetriesOnHostShouldBeZeroInitially() {
        assertNumberOfAttemptsOnHost(0, HOST_1);
        assertNumberOfAttemptsOnHost(0, HOST_2);
    }

    @Test
    public void shouldIncrementRetries() {
        request.triedOnHost(HOST_1);
        assertNumberOfTotalAttempts(1);
        assertNumberOfAttemptsOnHost(1, HOST_1);

        request.triedOnHost(HOST_1);
        assertNumberOfTotalAttempts(2);
        assertNumberOfAttemptsOnHost(2, HOST_1);
    }

    @Test
    public void shouldSeparateRetriesOnDifferentHosts() {
        request.triedOnHost(HOST_1);
        assertNumberOfTotalAttempts(1);
        assertNumberOfAttemptsOnHost(1, HOST_1);
        assertNumberOfAttemptsOnHost(0, HOST_2);

        request.triedOnHost(HOST_2);
        assertNumberOfTotalAttempts(2);
        assertNumberOfAttemptsOnHost(1, HOST_1);
        assertNumberOfAttemptsOnHost(1, HOST_2);
    }

    private void assertNumberOfTotalAttempts(int expected) {
        assertThat(request.getNumberOfAttempts()).isEqualTo(expected);
    }

    private void assertNumberOfAttemptsOnHost(int expected, InetSocketAddress host) {
        assertThat(request.getNumberOfAttemptsOnHost(host)).isEqualTo(expected);
    }

    private FunctionCheckedException<CassandraClient, Void, RuntimeException> noOp() {
        return new FunctionCheckedException<CassandraClient, Void, RuntimeException>() {
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
