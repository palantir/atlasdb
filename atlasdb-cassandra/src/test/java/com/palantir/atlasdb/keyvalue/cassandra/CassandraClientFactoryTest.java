/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.CassandraClientConfig;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.exception.SafeSSLPeerUnverifiedException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.security.auth.x500.X500Principal;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

public class CassandraClientFactoryTest {

    private static final CassandraServer DEFAULT_SERVER =
            CassandraServer.of(InetSocketAddress.createUnresolved("foo", 4000));

    private static final CassandraServer SERVER_TWO =
            CassandraServer.of(InetSocketAddress.createUnresolved("random", 50));
    private static final CassandraClientFactory FACTORY = new CassandraClientFactory(
            MetricsManagers.createForTests(),
            CassandraServer.of(InetSocketAddress.createUnresolved("localhost", 4242)),
            CassandraClientConfig.builder()
                    .socketTimeoutMillis(0)
                    .socketQueryTimeoutMillis(0)
                    .initialSocketQueryTimeoutMillis(0)
                    .credentials(ImmutableCassandraCredentialsConfig.builder()
                            .username("jeremy")
                            .password("tom")
                            .build())
                    .usingSsl(false)
                    .enableEndpointVerification(false)
                    .keyspace("ks")
                    .timeoutOnConnectionClose(Duration.ZERO)
                    .build());

    private static final InetAddress DEFAULT_ADDRESS = mockInetAddress("1.2.3.4");

    private CassandraClient client = mock(CassandraClient.class);
    private PooledObject<CassandraClient> pooledClient = new DefaultPooledObject<>(client);

    @Test
    public void validateObjectReliesOnOpennessOfUnderlyingTransport() throws TTransportException {
        when(client.getOutputProtocol()).thenReturn(new TCompactProtocol(new TMemoryInputTransport(), 31337, 131072));
        assertThat(FACTORY.validateObject(pooledClient)).isTrue();
    }

    @Test
    public void validateObjectDoesNotPropagateExceptionsThrown() {
        when(client.getOutputProtocol()).thenThrow(new RuntimeException());
        assertThat(FACTORY.validateObject(pooledClient)).isFalse();
    }

    @Test
    public void verifyEndpointDoesNotThrowWhenHostnameInCertificate() {
        SSLSocket sslSocket = createSSLSocket(DEFAULT_SERVER, DEFAULT_ADDRESS);
        assertThatCode(() -> CassandraClientFactory.verifyEndpoint(DEFAULT_SERVER, sslSocket, true))
                .doesNotThrowAnyException();
    }

    @Test
    public void verifyEndpointThrowsOnlyWhenConfiguredAndHostnameOrIpNotPresent() {
        SSLSocket sslSocket = createSSLSocket(DEFAULT_SERVER, DEFAULT_ADDRESS);
        assertThatThrownBy(() -> CassandraClientFactory.verifyEndpoint(SERVER_TWO, sslSocket, true))
                .isInstanceOf(SafeSSLPeerUnverifiedException.class);
        assertThatCode(() -> CassandraClientFactory.verifyEndpoint(SERVER_TWO, sslSocket, false))
                .doesNotThrowAnyException();
    }

    @Test
    public void verifyEndpointDoesNotThrowWhenHostnameNotPresentButIpIs() {
        String ipAddress = "1.1.1.1";
        String hostname = "foo-bar";
        InetSocketAddress inetSocketAddress = mockInetSocketAddress(ipAddress);
        CassandraServer cassandraServer = CassandraServer.of(hostname, inetSocketAddress);
        SSLSocket sslSocket =
                createSSLSocket(inetSocketAddress.getAddress(), "not-what-I-want", Optional.of(ipAddress));
        assertThatCode(() -> CassandraClientFactory.verifyEndpoint(cassandraServer, sslSocket, true))
                .doesNotThrowAnyException();
    }

    @Test
    public void verifyEndpointThrowsWhenSocketIsClosed() {
        SSLSocket sslSocket = createSSLSocket(DEFAULT_SERVER, DEFAULT_ADDRESS);
        when(sslSocket.isClosed()).thenReturn(true);
        assertThatThrownBy(() -> CassandraClientFactory.verifyEndpoint(DEFAULT_SERVER, sslSocket, true))
                .isInstanceOf(SafeSSLPeerUnverifiedException.class);
    }

    @Test
    public void verifyEndpointDoesNotThrowWhenSocketIsClosedAndThrowOnFailureIsFalse() {
        SSLSocket sslSocket = createSSLSocket(DEFAULT_SERVER, DEFAULT_ADDRESS);
        when(sslSocket.isClosed()).thenReturn(true);
        assertThatCode(() -> CassandraClientFactory.verifyEndpoint(DEFAULT_SERVER, sslSocket, false))
                .doesNotThrowAnyException();
    }

    @Test
    public void getEndpointsToCheckDeduplicatesMatchingHostnameIp() {
        CassandraServer cassandraServer =
                CassandraServer.of(InetSocketAddress.createUnresolved(DEFAULT_ADDRESS.getHostAddress(), 4000));
        SSLSocket sslSocket = createSSLSocket(cassandraServer, DEFAULT_ADDRESS);
        assertThat(CassandraClientFactory.getEndpointsToCheck(cassandraServer, sslSocket))
                .isNotEmpty()
                .containsExactly(DEFAULT_ADDRESS.getHostAddress());
    }

    @Test
    public void getEndpointsToCheckPerformsNoDeduplicationWhenHostnameIpDiffer() {
        SSLSocket sslSocket = createSSLSocket(DEFAULT_SERVER, DEFAULT_ADDRESS);
        assertThat(CassandraClientFactory.getEndpointsToCheck(DEFAULT_SERVER, sslSocket))
                .isNotEmpty()
                .containsExactlyInAnyOrder(DEFAULT_SERVER.cassandraHostName(), DEFAULT_ADDRESS.getHostAddress());
    }

    @SuppressWarnings("ReverseDnsLookup")
    private static InetSocketAddress mockInetSocketAddress(String ipAddress) {
        InetAddress inetAddress = mockInetAddress(ipAddress);
        InetSocketAddress inetSocketAddress = mock(InetSocketAddress.class);
        when(inetSocketAddress.getAddress()).thenReturn(inetAddress);
        return inetSocketAddress;
    }

    @SuppressWarnings("ReverseDnsLookup")
    private static InetAddress mockInetAddress(String ipAddress) {
        InetAddress inetAddress = mock(InetAddress.class);
        when(inetAddress.getHostAddress()).thenReturn(ipAddress);
        return inetAddress;
    }

    private static SSLSocket createSSLSocket(CassandraServer cassandraServer, InetAddress address) {
        return createSSLSocket(address, cassandraServer.cassandraHostName(), Optional.empty());
    }

    private static SSLSocket createSSLSocket(
            InetAddress inetAddressForSocket, String hostname, Optional<String> maybeIpAddressForCertificate) {
        SSLSocket sslSocket = mock(SSLSocket.class);
        when(sslSocket.getInetAddress()).thenReturn(inetAddressForSocket);
        SSLSession sslSession = mock(SSLSession.class);
        when(sslSocket.getSession()).thenReturn(sslSession);
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = mock(X500Principal.class);
        when(principal.toString()).thenReturn("CN=" + hostname);
        ImmutableList.Builder<List<?>> subjectAltsBuilder = ImmutableList.builder();
        maybeIpAddressForCertificate.ifPresent(ipAddress -> subjectAltsBuilder.add(List.of(7, ipAddress)));
        subjectAltsBuilder.add(List.of(2, hostname));
        Collection<List<?>> subjectAlts = subjectAltsBuilder.build();
        try {
            when(certificate.getSubjectAlternativeNames()).thenReturn(subjectAlts);
        } catch (CertificateParsingException e) {
            throw new RuntimeException(e);
        }
        when(certificate.getSubjectX500Principal()).thenReturn(principal);
        try {
            when(sslSession.getPeerCertificates()).thenReturn(new Certificate[] {certificate});
        } catch (SSLPeerUnverifiedException e) {
            throw new RuntimeException(e);
        }
        return sslSocket;
    }
}
