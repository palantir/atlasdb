/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

public class ExceptionCountingRefreshingClientTest {
    private Supplier<Client> clientSupplier = (Supplier<Client>) mock(Supplier.class);
    private Client client = mock(Client.class);

    private static final Request request = Request.create("POST", "atlas.palantir.pt", ImmutableMap.of(), null, null);
    private static final Response response = Response.create(204, "no content", ImmutableMap.of(), new byte[0]);
    private static final Request.Options options = new Request.Options(1, 1);

    private static final int EXCEPTION_COUNT_BEFORE_REFRESH = 2;
    private static final RuntimeException CLIENT_EXCEPTION = new RuntimeException("bad");

    @Test
    public void passesRequestsThroughToTheDelegate() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options)).thenReturn(response);

        Client refreshingClient = ExceptionCountingRefreshingClient.createRefreshingClient(clientSupplier);
        callExecute(refreshingClient, 1);

        verify(client, times(1)).execute(request, options);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void createsClientOnlyOnceIfMakingMultipleRequests() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options)).thenReturn(response);

        Client refreshingClient = ExceptionCountingRefreshingClient.createRefreshingClient(clientSupplier);
        callExecute(refreshingClient, 3);

        verify(clientSupplier, times(1)).get();
        verify(client, times(3)).execute(request, options);
        verifyNoMoreInteractions(clientSupplier, client);
    }

    @Test
    public void reinvokesSupplierAfterTheSpecifiedNumberOfExceptionsThrown() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options)).thenThrow(CLIENT_EXCEPTION);

        Client refreshingClient = new ExceptionCountingRefreshingClient(clientSupplier, EXCEPTION_COUNT_BEFORE_REFRESH);

        int numberOfExecutions = EXCEPTION_COUNT_BEFORE_REFRESH + 1;
        callExecute(refreshingClient, numberOfExecutions);

        verify(clientSupplier, times(numberOfExecutions / EXCEPTION_COUNT_BEFORE_REFRESH + 1)).get();
        verify(client, times(numberOfExecutions)).execute(request, options);
        verifyNoMoreInteractions(clientSupplier, client);
    }

    @Test
    public void requestsContinueWithOldClientIfDelegateSupplierThrows() throws IOException {
        when(clientSupplier.get()).thenReturn(client).thenThrow(new IllegalStateException("bad"));
        when(client.execute(request, options)).thenThrow(CLIENT_EXCEPTION);

        Client refreshingClient = new ExceptionCountingRefreshingClient(clientSupplier, EXCEPTION_COUNT_BEFORE_REFRESH);

        int numberOfExecutions = EXCEPTION_COUNT_BEFORE_REFRESH + 1;
        callExecute(refreshingClient, numberOfExecutions);

        verify(clientSupplier, times(numberOfExecutions / EXCEPTION_COUNT_BEFORE_REFRESH + 1)).get();
        verify(client, times(numberOfExecutions)).execute(request, options);
        verifyNoMoreInteractions(clientSupplier, client);
    }

    @Test
    public void doesNotRefreshClientIfExceptionsAreNotThrownInARow() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options))
                .thenThrow(CLIENT_EXCEPTION)
                .thenReturn(response)
                .thenThrow(CLIENT_EXCEPTION)
                .thenReturn(response);

        Client refreshingClient = new ExceptionCountingRefreshingClient(clientSupplier, EXCEPTION_COUNT_BEFORE_REFRESH);

        int numberOfExecutions = 4;
        callExecute(refreshingClient, numberOfExecutions);

        verify(clientSupplier, times(1)).get();
        verify(client, times(numberOfExecutions)).execute(request, options);
    }

    @Test
    public void doesRefreshClientIfExceptionsAreThrownInARow() throws IOException {
        when(clientSupplier.get()).thenReturn(client);
        when(client.execute(request, options))
                .thenReturn(response)
                .thenThrow(CLIENT_EXCEPTION)
                .thenThrow(CLIENT_EXCEPTION)
                .thenReturn(response);

        Client refreshingClient = new ExceptionCountingRefreshingClient(clientSupplier, EXCEPTION_COUNT_BEFORE_REFRESH);

        int numberOfExecutions = 4;
        callExecute(refreshingClient, numberOfExecutions);

        verify(clientSupplier, times(2)).get();
        verify(client, times(numberOfExecutions)).execute(request, options);
    }

    private void callExecute(Client client, int numberOfExecutions) throws IOException {
        for (int i = 0; i < numberOfExecutions; i++) {
            try {
                client.execute(request, options);
            } catch (RuntimeException e) {
                // ignored - TODO(gsheasby): Should check exception content
            }
        }
    }
}
