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
package com.palantir.atlasdb.ete.suites;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.containers.CassandraEnvironment;
import com.palantir.atlasdb.ete.EteSetup;
import com.palantir.atlasdb.ete.ServiceExposureEteTest;
import com.palantir.atlasdb.ete.TimestampManagementEteTest;
import com.palantir.atlasdb.ete.TodoEteTest;
import com.palantir.atlasdb.ete.coordination.CoordinationEteTest;
import com.palantir.atlasdb.ete.coordination.MultipleSchemaVersionsCoordinationEteTest;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    TodoEteTest.class,
    ServiceExposureEteTest.class,
    TimestampManagementEteTest.class,
    CoordinationEteTest.class,
    MultipleSchemaVersionsCoordinationEteTest.class
})
public class SingleClientWithEmbeddedAndThreeNodeCassandraTestSuite extends EteSetup {
    @ClassRule
    public static final RuleChain COMPOSITION_SETUP = EteSetup.setupComposition(
            SingleClientWithEmbeddedAndThreeNodeCassandraTestSuite.class,
            "docker-compose.single-client-with-embedded-and-three-node-cassandra.yml",
            TestSuites.SINGLE_CLIENT,
            CassandraEnvironment.get());
}
