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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.Test;

public class TimeLockResourceTest {
    private static final String CLIENT_A = "a-client";
    private static final String CLIENT_B = "b-client";

    private static final int DEFAULT_MAX_NUMBER_OF_CLIENTS = 5;

    private final TimeLockServices servicesA = mock(TimeLockServices.class);
    private final TimeLockServices servicesB = mock(TimeLockServices.class);

    private final Function<String, TimeLockServices> serviceFactory = mock(Function.class);
    private final Supplier<Integer> maxNumberOfClientsSupplier = mock(Supplier.class);
    private final TimeLockResource resource = new TimeLockResource(
            serviceFactory,
            maxNumberOfClientsSupplier);


    @Before
    public void before() {
        when(serviceFactory.apply(any())).thenReturn(mock(TimeLockServices.class));
        when(serviceFactory.apply(CLIENT_A)).thenReturn(servicesA);
        when(serviceFactory.apply(CLIENT_B)).thenReturn(servicesB);

        when(maxNumberOfClientsSupplier.get()).thenReturn(DEFAULT_MAX_NUMBER_OF_CLIENTS);
    }

    @Test
    public void returnsProperServiceForEachClient() {
        assertThat(resource.getOrCreateServices(CLIENT_A)).isEqualTo(servicesA);
        assertThat(resource.getOrCreateServices(CLIENT_B)).isEqualTo(servicesB);
    }

    @Test
    public void servicesAreOnlyCreatedOncePerClient() {
        resource.getTimeService(CLIENT_A);
        resource.getTimeService(CLIENT_A);

        verify(serviceFactory, times(1)).apply(any());
    }

    @Test
    public void doesNotCreateNewClientsAfterMaximumNumberHasBeenReached() {
        createMaximumNumberOfClients();

        assertThatThrownBy(() -> resource.getTimeService(uniqueClient()))
                .isInstanceOf(IllegalStateException.class);

        verify(serviceFactory, times(DEFAULT_MAX_NUMBER_OF_CLIENTS)).apply(any());
        verifyNoMoreInteractions(serviceFactory);
    }

    @Test
    public void returnsMaxNumberOfClients() {
        createMaximumNumberOfClients();
        assertThat(resource.numberOfClients()).isEqualTo(DEFAULT_MAX_NUMBER_OF_CLIENTS);
    }

    @Test
    public void onClientCreationIncreaseNumberOfClients() {
        assertThat(resource.numberOfClients()).isEqualTo(0);
        resource.getTimeService(uniqueClient());
        assertThat(resource.numberOfClients()).isEqualTo(1);
    }

    @Test
    public void canDynamicallyIncreaseMaxAllowedClients() {
        createMaximumNumberOfClients();

        when(maxNumberOfClientsSupplier.get()).thenReturn(DEFAULT_MAX_NUMBER_OF_CLIENTS + 1);

        resource.getTimeService(uniqueClient());
    }

    private void createMaximumNumberOfClients() {
        for (int i = 0; i < DEFAULT_MAX_NUMBER_OF_CLIENTS; i++) {
            resource.getTimeService(uniqueClient());
        }
    }

    private String uniqueClient() {
        return UUID.randomUUID().toString();
    }

}
