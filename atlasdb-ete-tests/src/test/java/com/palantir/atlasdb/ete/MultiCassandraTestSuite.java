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
package com.palantir.atlasdb.ete;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.ete.coordination.CoordinationEteTest;
import com.palantir.atlasdb.ete.coordination.MultipleSchemaVersionsCoordinationEteTest;
import java.util.List;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    TodoEteTest.class,
    ServiceExposureEteTest.class,
    MultiCassandraSingleNodeDownEteTest.class,
    MultiCassandraDoubleNodeDownEteTest.class,
    TimestampManagementEteTest.class,
    CoordinationEteTest.class,
    MultipleSchemaVersionsCoordinationEteTest.class,
    LockWithoutTimelockEteTest.class
})
public class MultiCassandraTestSuite extends EteSetup {
    private static final List<String> CLIENTS = ImmutableList.of("ete1");

    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            MultiCassandraTestSuite.class,
            "docker-compose.multiple-cassandra.yml",
            CLIENTS,
            CassandraEnvironment.get());
}
