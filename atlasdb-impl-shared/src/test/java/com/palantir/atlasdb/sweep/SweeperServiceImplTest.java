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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.Response;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.keyvalue.api.SweepResults;
import com.palantir.atlasdb.persistentlock.CheckAndSetExceptionMapper;
import com.palantir.atlasdb.persistentlock.KvsBackedPersistentLockServiceClientTest;
import com.palantir.atlasdb.sweep.progress.SweepProgress;
import com.palantir.atlasdb.sweeperservice.SweeperService;
import com.palantir.remoting2.clients.UserAgents;
import com.palantir.remoting2.errors.RemoteException;
import com.palantir.remoting2.jaxrs.JaxRsClient;
import com.palantir.remoting2.servers.jersey.HttpRemotingJerseyFeature;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class SweeperServiceImplTest extends SweeperTestSetup {

    SweeperService sweeperService;

    @Rule
    public DropwizardClientRule dropwizardClientRule = new DropwizardClientRule(
            new SweeperServiceImpl(getSpecificTableSweeperService()),
            new CheckAndSetExceptionMapper(),
            HttpRemotingJerseyFeature.DEFAULT);

    @Before
    public void setup() {
        sweeperService = JaxRsClient.builder().build(
                SweeperService.class,
                UserAgents.fromClass(KvsBackedPersistentLockServiceClientTest.class, "test", "unknown"),
                dropwizardClientRule.baseUri().toString());
    }

    @Test
    public void sweepingNonFullyTableShouldNotBeSuccessful() {
        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> sweeperService.sweepTable("non_fully_qualified_name"))
                .matches(ex -> ex.getStatus() == Response.Status.BAD_REQUEST.getStatusCode());
    }


    @Test
    public void sweepingNonExistingTableShouldNotBeSuccessful() {
        assertThatExceptionOfType(RemoteException.class)
                .isThrownBy(() -> sweeperService.sweepTable("ns.non_existing_table"))
                .matches(ex -> ex.getStatus() == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }

    @Test
    public void testWriteProgressOrPriorityOrMetricsNotUpdatedAfterSweepRunsSuccessfully() {
        setProgress(Mockito.mock(SweepProgress.class));
        setupTaskRunner(Mockito.mock(SweepResults.class));
        when(kvs.getAllTableNames()).thenReturn(ImmutableSet.of(TABLE_REF));
        assertTrue(sweeperService.sweepTableFromStartRow(TABLE_REF.getQualifiedName(), encodeStartRow(new byte[] {1, 2, 3})));
        Mockito.verify(priorityStore, never()).update(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(progressStore, never()).saveProgress(Mockito.any(), Mockito.any());
        Mockito.verifyZeroInteractions(sweepMetrics);
    }

    private String encodeStartRow(byte[] rowBytes) {
        return BaseEncoding.base16().encode(rowBytes);
    }
}
