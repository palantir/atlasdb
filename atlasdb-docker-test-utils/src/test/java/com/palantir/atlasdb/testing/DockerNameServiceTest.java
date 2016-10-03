/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.testing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Supplier;

import org.junit.Test;

import com.google.common.net.InetAddresses;

public class DockerNameServiceTest {
    private static final InetAddress HOST_NAME_ADDRESS = InetAddresses.forString("127.0.0.1");
    private static final String HOST_NAME = "host";

    private final Supplier<ProjectInfoMappings> mappings = mock(Supplier.class);
    private final DockerNameService dockerNameService = new DockerNameService(mappings);

    @Test
    public void shouldReturnIpOfHost() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putHostToIp(HOST_NAME, HOST_NAME_ADDRESS)
                .build());

        assertThat(dockerNameService.lookupAllHostAddr(HOST_NAME)).containsExactly(HOST_NAME_ADDRESS);
    }

    @Test
    public void shouldOnlyQueryTheSupplierOncePerLookupCall() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putHostToIp(HOST_NAME, HOST_NAME_ADDRESS)
                .build());

        dockerNameService.lookupAllHostAddr(HOST_NAME);

        verify(mappings, times(1)).get();
    }

    @Test
    public void shouldGetIpOfHostFromSupplierEveryTime() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putHostToIp(HOST_NAME, HOST_NAME_ADDRESS)
                .build());

        dockerNameService.lookupAllHostAddr(HOST_NAME);
        dockerNameService.lookupAllHostAddr(HOST_NAME);

        verify(mappings, times(2)).get();
    }

    @Test(expected = UnknownHostException.class)
    public void shouldThrowUnknownHostExceptionWhenNoIpForHost() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .build());

        dockerNameService.lookupAllHostAddr(HOST_NAME);
    }

    @Test
    public void shouldGetHostFromIp() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putIpToHosts(HOST_NAME_ADDRESS.getHostAddress(), HOST_NAME)
                .build());

        assertThat(dockerNameService.getHostByAddr(HOST_NAME_ADDRESS.getAddress())).isEqualTo(HOST_NAME);
    }

    @Test
    public void shouldOnlyQueryTheSupplierOncePerHostByAddrCall() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putIpToHosts(HOST_NAME_ADDRESS.getHostAddress(), HOST_NAME)
                .build());

        dockerNameService.getHostByAddr(HOST_NAME_ADDRESS.getAddress());

        verify(mappings, times(1)).get();
    }

    @Test
    public void shouldGetHostOfIpFromSupplierEveryTime() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .putIpToHosts(HOST_NAME_ADDRESS.getHostAddress(), HOST_NAME)
                .build());

        dockerNameService.getHostByAddr(HOST_NAME_ADDRESS.getAddress());
        dockerNameService.getHostByAddr(HOST_NAME_ADDRESS.getAddress());

        verify(mappings, times(2)).get();
    }

    @Test(expected = UnknownHostException.class)
    public void shouldThrowUnknownHostExceptionWhenNoHostForIp() throws UnknownHostException {
        when(mappings.get()).thenReturn(ImmutableProjectInfoMappings.builder()
                .build());

        dockerNameService.getHostByAddr(HOST_NAME_ADDRESS.getAddress());
    }
}
