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

package com.palantir.atlasdb.cassandra;

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReloadingCloseableContainerTest {
    private static final int INITIAL_VALUE = 0;
    private static final int UPDATED_VALUE = 1;

    @Mock
    private Function<Integer, java.lang.AutoCloseable> resourceFactory;

    private java.lang.AutoCloseable initialResource;

    private java.lang.AutoCloseable refreshedResource;

    private SettableRefreshable<Integer> refreshableFactoryArg;

    private ReloadingCloseableContainer<java.lang.AutoCloseable> reloadingCloseableContainer;

    @Before
    public void setUp() {
        initialResource = mockFactory(INITIAL_VALUE);
        refreshedResource = mockFactory(UPDATED_VALUE);
        refreshableFactoryArg = Refreshable.create(INITIAL_VALUE);
        reloadingCloseableContainer = ReloadingCloseableContainer.of(refreshableFactoryArg, resourceFactory);
    }

    @Test
    public void lastCqlClusterClosedAfterClose() throws Exception {
        java.lang.AutoCloseable resource = reloadingCloseableContainer.get();
        reloadingCloseableContainer.close();
        verify(resource).close();
    }

    @Test
    public void previousCqlClusterIsClosedAfterRefresh() throws Exception {
        java.lang.AutoCloseable resource = reloadingCloseableContainer.get();
        refreshableFactoryArg.update(UPDATED_VALUE);
        verify(resource).close();
    }

    @Test
    public void newCqlClusterCreatedWithNewServerListAfterRefresh() throws Exception {
        AutoCloseable resource = reloadingCloseableContainer.get();

        refreshableFactoryArg.update(INITIAL_VALUE);

        AutoCloseable secondResource = reloadingCloseableContainer.get();

        assertThat(resource).isEqualTo(initialResource);
        assertThat(secondResource).isEqualTo(refreshedResource);
        verify(secondResource, never()).close();
    }

    @Test
    public void noNewClustersAfterClose() {
        reloadingCloseableContainer.close();
        refreshableFactoryArg.update(UPDATED_VALUE);
        assertThat(reloadingCloseableContainer.get()).isEqualTo(initialResource);
        verify(resourceFactory, never()).apply(UPDATED_VALUE);
    }

    private AutoCloseable mockFactory(int factoryArg) {
        java.lang.AutoCloseable autoCloseable = mock(java.lang.AutoCloseable.class);
        when(resourceFactory.apply(factoryArg)).thenReturn(autoCloseable);
        return autoCloseable;
    }
}
