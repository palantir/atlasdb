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

package com.palantir.atlasdb.ete.coordination;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.coordination.CoordinationResource;
import com.palantir.atlasdb.ete.EteSetup;
import org.junit.Before;
import org.junit.Test;

public class CoordinationEteTest {
    private static final int VERSION_ONE = 1;
    private static final int NEW_VERSION = 5888888;

    private final CoordinationResource coordinationResource =
            EteSetup.createClientToSingleNode(CoordinationResource.class);

    private long lowerBoundOnTimestamps;

    @Before
    public void setUp() {
        lowerBoundOnTimestamps = coordinationResource.resetStateAndGetFreshTimestamp();
    }

    @Test
    public void defaultTransactionsSchemaVersionIsOne() {
        assertThat(coordinationResource.getTransactionsSchemaVersion(lowerBoundOnTimestamps))
                .isEqualTo(1);
    }

    @Test
    public void transactionSucceedsUnderKnownSchemaVersion() {
        assertTransactionsSchemaVersionIsNow(VERSION_ONE, coordinationResource);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isTrue();
    }

    @Test
    public void transactionOnKnownVersionAndFreshCellSucceedsEvenIfWePassedThroughAnUnknownVersion() {
        coordinationResource.forceInstallNewTransactionsSchemaVersion(NEW_VERSION);
        assertTransactionsSchemaVersionIsNow(NEW_VERSION, coordinationResource);
        coordinationResource.forceInstallNewTransactionsSchemaVersion(VERSION_ONE);
        assertTransactionsSchemaVersionIsNow(VERSION_ONE, coordinationResource);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isTrue();
    }

    public static void assertTransactionsSchemaVersionIsNow(int expectedVersion, CoordinationResource resource) {
        assertThat(resource.getTransactionsSchemaVersion(resource.getFreshTimestamp()))
                .isEqualTo(expectedVersion);
    }
}
