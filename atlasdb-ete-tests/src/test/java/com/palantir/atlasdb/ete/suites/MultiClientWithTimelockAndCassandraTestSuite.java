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

import com.palantir.atlasdb.ete.tests.MultiClientWithTimelockAndCassandraCassandraTimestampsTest;
import com.palantir.atlasdb.ete.tests.MultiClientWithTimelockAndCassandraCoordinationTest;
import com.palantir.atlasdb.ete.tests.MultiClientWithTimelockAndCassandraLockWithTimelockTest;
import com.palantir.atlasdb.ete.tests.MultiClientWithTimelockAndCassandraMultipleSchemaVersionsCoordinationTest;
import com.palantir.atlasdb.ete.tests.MultiClientWithTimelockAndCassandraTargetedSweepTest;
import com.palantir.atlasdb.ete.tests.MultiClientWithTimelockAndCassandraTimestampManagementTest;
import com.palantir.atlasdb.ete.tests.MultiClientWithTimelockAndCassandraTodoTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
    MultiClientWithTimelockAndCassandraTodoTest.class,
    MultiClientWithTimelockAndCassandraTargetedSweepTest.class,
    MultiClientWithTimelockAndCassandraCassandraTimestampsTest.class,
    MultiClientWithTimelockAndCassandraTimestampManagementTest.class,
    MultiClientWithTimelockAndCassandraCoordinationTest.class,
    MultiClientWithTimelockAndCassandraMultipleSchemaVersionsCoordinationTest.class,
    MultiClientWithTimelockAndCassandraLockWithTimelockTest.class
})
public class MultiClientWithTimelockAndCassandraTestSuite {}
