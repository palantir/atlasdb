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
import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
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
    private Function<Integer, AutoCloseable> resourceFactory;

    @Mock
    private AutoCloseable initialResource;

    @Mock
    private AutoCloseable refreshedResource;

    private SettableRefreshable<Integer> refreshableFactoryArg;

    private ReloadingCloseableContainer<AutoCloseable> reloadingCloseableContainer;

    @Before
    public void setUp() {
        when(resourceFactory.apply(INITIAL_VALUE)).thenReturn(initialResource);
        when(resourceFactory.apply(UPDATED_VALUE)).thenReturn(refreshedResource);
        refreshableFactoryArg = Refreshable.create(INITIAL_VALUE);
        reloadingCloseableContainer = ReloadingCloseableContainer.of(refreshableFactoryArg, resourceFactory);
    }

    @Test
    public void lastResourceClosedAfterClose() throws Exception {
        AutoCloseable resource = reloadingCloseableContainer.get();
        reloadingCloseableContainer.close();
        verify(resource).close();
    }

    @Test
    public void previousResourceIsClosedAfterRefresh() throws Exception {
        AutoCloseable resource = reloadingCloseableContainer.get();
        refreshableFactoryArg.update(UPDATED_VALUE);
        verify(resource).close();
    }

    @Test
    public void newResourceCreatedWithUpdatedRefreshableValueAfterRefresh() throws Exception {
        AutoCloseable resource = reloadingCloseableContainer.get();

        refreshableFactoryArg.update(UPDATED_VALUE);

        AutoCloseable secondResource = reloadingCloseableContainer.get();

        assertThat(resource).isEqualTo(initialResource);
        assertThat(secondResource).isEqualTo(refreshedResource);
        verify(secondResource, never()).close();
    }

    @Test
    public void noNewResourcesAfterClose() {
        reloadingCloseableContainer.close();
        refreshableFactoryArg.update(UPDATED_VALUE);
        verify(resourceFactory, never()).apply(UPDATED_VALUE);
    }

    @Test
    public void getAfterCloseThrowsIllegalStateException() {
        reloadingCloseableContainer.close();
        assertThatLoggableExceptionThrownBy(reloadingCloseableContainer::get)
                .isInstanceOf(SafeIllegalStateException.class)
                .hasLogMessage("Attempted to get a resource after the container was closed");
    }
}
