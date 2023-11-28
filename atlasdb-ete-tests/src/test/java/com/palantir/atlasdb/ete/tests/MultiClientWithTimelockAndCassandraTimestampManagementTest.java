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
package com.palantir.atlasdb.ete.tests;

import com.palantir.atlasdb.ete.suiteclasses.AbstractTimestampManagementTest;
import com.palantir.atlasdb.ete.utilities.EteExtension;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MultiClientWithTimelockAndCassandraTimestampManagementTest extends AbstractTimestampManagementTest {
    @RegisterExtension
    public static EteExtension eteExtension =
            EteExtension.getInstance(EteExtension.MULTI_CLIENT_WITH_TIMELOCK_AND_CASSANDRA_ETE_EXTENSION_CONFIGURATION);
}
