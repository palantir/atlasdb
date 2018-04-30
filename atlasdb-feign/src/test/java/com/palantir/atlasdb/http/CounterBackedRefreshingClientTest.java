/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import feign.Client;
import feign.Request;
import feign.Response;

public class CounterBackedRefreshingClientTest {
    @SuppressWarnings("unchecked") // For testing, and we can configure the return types appropriately.
    private Supplier<Client> clientSupplier = (Supplier<Client>) mock(Supplier.class);
    private Client client = mock(Client.class);

    private Request request = Request.create("POST", "atlas.palantir.pt", ImmutableMap.of(), null, null);
    private Request.Options options = new Request.Options(1, 1);

    @Test
    public void passesRequestsThroughToTheDelegate() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options)).thenReturn(
                Response.create(204, "no content", ImmutableMap.of(), new byte[0]));

        Client refreshingClient = CounterBackedRefreshingClient.createRefreshingClient(clientSupplier);
        refreshingClient.execute(request, options);

        verify(client, times(1)).execute(request, options);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void createsClientOnlyOnceIfMakingMultipleRequests() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options)).thenReturn(
                Response.create(204, "no content", ImmutableMap.of(), new byte[0]));

        Client refreshingClient = CounterBackedRefreshingClient.createRefreshingClient(clientSupplier);
        refreshingClient.execute(request, options);
        refreshingClient.execute(request, options);
        refreshingClient.execute(request, options);

        verify(clientSupplier).get();
        verify(client, times(3)).execute(request, options);
        verifyNoMoreInteractions(clientSupplier, client);
    }

    @Test
    public void reinvokesSupplierAfterTheSpecifiedNumberOfRequestsHaveBeenMade() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options)).thenReturn(
                Response.create(204, "no content", ImmutableMap.of(), new byte[0]));

        Client refreshingClient = new CounterBackedRefreshingClient(clientSupplier, 2);
        refreshingClient.execute(request, options);
        refreshingClient.execute(request, options);
        refreshingClient.execute(request, options);

        verify(clientSupplier, times(2)).get();
        verify(client, times(3)).execute(request, options);
        verifyNoMoreInteractions(clientSupplier, client);
    }

    @Test
    public void requestsContinueWithOldClientIfDelegateSupplierThrows() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options)).thenReturn(
                Response.create(204, "no content", ImmutableMap.of(), new byte[0]));

        Client refreshingClient = new CounterBackedRefreshingClient(clientSupplier, 2);
        refreshingClient.execute(request, options);

        when(clientSupplier.get()).thenThrow(new IllegalStateException("bad"));
        refreshingClient.execute(request, options);
        refreshingClient.execute(request, options); // Creation failed, so we delegate to the old client still.

        verify(clientSupplier, times(2)).get();
        verify(client, times(3)).execute(request, options);
        verifyNoMoreInteractions(clientSupplier, client);
    }
}
