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

package com.palantir.atlasdb.cassandra.backup;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CqlClusterTest {
    @Mock
    private Cluster cluster;

    @Test
    public void retriesSessionFetching() {
        Session session = mock(Session.class);

        when(cluster.connect())
                .thenThrow(new NoHostAvailableException(ImmutableMap.of()))
                .thenReturn(session);

        CqlCluster cqlCluster = new CqlCluster(
                cluster,
                BackupTestUtils.cqlCapableConfig("host1", "host2"),
                Namespace.of("namespace"),
                Duration.ofMillis(50L));

        assertThatCode(cqlCluster::createSessionWithRetry).doesNotThrowAnyException();
    }
}
