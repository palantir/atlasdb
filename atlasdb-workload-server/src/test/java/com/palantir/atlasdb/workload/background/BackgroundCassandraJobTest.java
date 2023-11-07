/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.background;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.palantir.atlasdb.buggify.AlwaysBuggifyFactory;
import com.palantir.atlasdb.workload.resource.CassandraSidecarResource;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class BackgroundCassandraJobTest {

    private static final String HOST_ONE = "cassandra1";
    private static final String HOST_TWO = "cassandra2";
    private static final String HOST_THREE = "cassandra3";

    private static final List<String> CASSANDRA_HOSTS = List.of(HOST_ONE, HOST_TWO, HOST_THREE);

    @Mock
    private CassandraSidecarResource cassandraSidecarResource;

    @Test
    public void runCanExecuteCompactFlushOnAllCassandraHosts() {
        BackgroundCassandraJob backgroundCassandraJob =
                new BackgroundCassandraJob(CASSANDRA_HOSTS, cassandraSidecarResource, AlwaysBuggifyFactory.INSTANCE);
        CASSANDRA_HOSTS.forEach(_ignore -> backgroundCassandraJob.run());
        CASSANDRA_HOSTS.forEach(host -> verify(cassandraSidecarResource).compact(eq(host)));
        CASSANDRA_HOSTS.forEach(host -> verify(cassandraSidecarResource).flush(eq(host)));
    }
}
