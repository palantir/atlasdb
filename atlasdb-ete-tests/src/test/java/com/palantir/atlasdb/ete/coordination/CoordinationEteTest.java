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

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.coordination.CoordinationResource;
import com.palantir.atlasdb.ete.EteSetup;

public class CoordinationEteTest {
    private static final int VERSION_ONE = 1;
    private static final int NEW_VERSION = 5888888;

    private final CoordinationResource coordinationResource
            = EteSetup.createClientToSingleNode(CoordinationResource.class);

    private long lowerBoundOnTimestamps;

    @Before
    public void setUp() {
        lowerBoundOnTimestamps = coordinationResource.resetStateAndGetFreshTimestamp();
    }

    @Test
    public void defaultTransactionsSchemaVersionIsOne() {
        assertThat(coordinationResource.getTransactionsSchemaVersion(lowerBoundOnTimestamps)).isEqualTo(1);
    }

    @Test
    public void transactionSucceedsUnderKnownSchemaVersion() {
        assertThat(coordinationResource.getTransactionsSchemaVersion(coordinationResource.getFreshTimestamp()))
                .isEqualTo(VERSION_ONE);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isTrue();
    }

    @Test
    public void transactionFailsUnderUnknownSchemaVersion() {
        coordinationResource.forceInstallNewTransactionsSchemaVersion(NEW_VERSION);
        assertThat(coordinationResource.getTransactionsSchemaVersion(coordinationResource.getFreshTimestamp()))
                .isEqualTo(NEW_VERSION);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isFalse();
    }

    @Test
    public void tryInstallNewVersionDoesNotForceImmediateChangeover() {
        coordinationResource.tryInstallNewTransactionsSchemaVersion(NEW_VERSION);
        assertThat(coordinationResource.getTransactionsSchemaVersion(coordinationResource.getFreshTimestamp()))
                .isEqualTo(VERSION_ONE);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isTrue();
    }

    @Test
    public void transactionOnKnownVersionFailsOnValueWithUnknownVersion() {
        coordinationResource.forceInstallNewTransactionsSchemaVersion(NEW_VERSION);
        assertThat(coordinationResource.getTransactionsSchemaVersion(coordinationResource.getFreshTimestamp()))
                .isEqualTo(NEW_VERSION);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isFalse();
        coordinationResource.forceInstallNewTransactionsSchemaVersion(VERSION_ONE);
        assertThat(coordinationResource.getTransactionsSchemaVersion(coordinationResource.getFreshTimestamp()))
                .isEqualTo(VERSION_ONE);

        // This must still determine whether the transaction that started under a NEW_VERSION regime committed
        // or not. We can't tell, hence we must fail.
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isFalse();
    }

    @Test
    public void transactionOnKnownVersionAndFreshCellSucceedsEvenIfWePassedThroughAnUnknownVersion() {
        coordinationResource.forceInstallNewTransactionsSchemaVersion(NEW_VERSION);
        assertThat(coordinationResource.getTransactionsSchemaVersion(coordinationResource.getFreshTimestamp()))
                .isEqualTo(NEW_VERSION);
        coordinationResource.forceInstallNewTransactionsSchemaVersion(VERSION_ONE);
        assertThat(coordinationResource.getTransactionsSchemaVersion(coordinationResource.getFreshTimestamp()))
                .isEqualTo(VERSION_ONE);
        assertThat(coordinationResource.doTransactionAndReportOutcome()).isTrue();
    }
}
