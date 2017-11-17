/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.List;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.todo.ImmutableTodo;
import com.palantir.atlasdb.todo.Todo;
import com.palantir.atlasdb.todo.TodoResource;
import com.palantir.atlasdb.util.AtlasDbMetrics;

public class QosCassandraTestSuite extends EteSetup {
    private static final List<String> CLIENTS = ImmutableList.of("ete1");
    private static final Todo TODO = ImmutableTodo.of(String.join("", Collections.nCopies(100_000, "a")));

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            QosCassandraTestSuite.class,
            "docker-compose.qos.cassandra.yml",
            CLIENTS,
            CassandraEnvironment.get());

    @Test
    public void shouldFailIfWritingTooManyBytes() {
        TodoResource todoClient = EteSetup.createClientToSingleNode(TodoResource.class);
        todoClient.addTodo(TODO);
        assertThatThrownBy(() -> todoClient.addTodo(TODO))
                .isInstanceOf(RuntimeException.class);
        Meter meter = AtlasDbMetrics.getMetricRegistry().getMeters().get(
                "com.palantir.atlasdb.qos.metrics.QosMetrics.numReadRequests");
        assertThat(meter.getCount()).isGreaterThan(200_000).isLessThan(201_000);
    }
}
