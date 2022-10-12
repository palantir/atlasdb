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
import java.util.Optional;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TMemoryInputTransport;
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

    private CassandraClient client = mock(CassandraClient.class);
    private PooledObject<CassandraClient> pooledClient = new DefaultPooledObject<>(client);

    @Test
    public void reliesOnOpennessOfUnderlyingTransport() {
        when(client.getOutputProtocol()).thenReturn(new TCompactProtocol(new TMemoryInputTransport(), 31337, 131072));
        assertThat(FACTORY.validateObject(pooledClient)).isTrue();
    }

    @Test
    public void doesNotPropagateExceptionsThrown() {
        when(client.getOutputProtocol()).thenThrow(new RuntimeException());
        assertThat(FACTORY.validateObject(pooledClient)).isFalse();
    }

    @Test
    public void verifyEndpoint_doesNotThrow_whenHostnameInCertificate() throws SSLPeerUnverifiedException {
        SSLSession sslSession = createSSLSession(DEFAULT_SERVER);
        CassandraClientFactory.verifyEndpoint(DEFAULT_SERVER, sslSession, true);
    }

    @Test
    public void verifyEndpoint_throws_onlyWhenConfigured_andHostname_orIpNotPresent()
            throws SSLPeerUnverifiedException {
        SSLSession sslSession = createSSLSession(DEFAULT_SERVER);
        assertThatThrownBy(() -> CassandraClientFactory.verifyEndpoint(SERVER_TWO, sslSession, true))
                .isInstanceOf(SafeSSLPeerUnverifiedException.class);
        CassandraClientFactory.verifyEndpoint(SERVER_TWO, sslSession, false);
    }

    @Test
    public void verifyEndpoint_doesNotThrow_whenHostnameNotPresentButIpIs() throws SSLPeerUnverifiedException {
        String ipAddress = "1.1.1.1";
        String hostname = "foo-bar";
        InetSocketAddress inetSocketAddress = mockInetSocketAddress(hostname, ipAddress);
        CassandraServer cassandraServer = CassandraServer.of(hostname, inetSocketAddress);
        SSLSession sslSession = createSSLSession("not-what-I-want", Optional.of(ipAddress));
        CassandraClientFactory.verifyEndpoint(cassandraServer, sslSession, true);
    }

    @Test
    public void maybeResolveAddress_emptyOnUnresolvableAddress() {
        assertThat(CassandraClientFactory.maybeResolveAddress(DEFAULT_SERVER.proxy()))
                .isEmpty();
    }

    @Test
    public void maybeResolveAddress_resolvesIfUnresolved() {
        InetSocketAddress localhostUnresolved = InetSocketAddress.createUnresolved("localhost", 80);
        assertThat(localhostUnresolved.getAddress()).isNull();
        assertThat(CassandraClientFactory.maybeResolveAddress(localhostUnresolved))
                .isPresent();
    }

    @SuppressWarnings("ReverseDnsLookup")
    private InetSocketAddress mockInetSocketAddress(String hostname, String ipAddress) {
        InetAddress inetAddress = mock(InetAddress.class);
        when(inetAddress.getHostAddress()).thenReturn(ipAddress);
        when(inetAddress.getHostName()).thenReturn(hostname);
        InetSocketAddress inetSocketAddress = mock(InetSocketAddress.class);
        when(inetSocketAddress.getAddress()).thenReturn(inetAddress);
        return inetSocketAddress;
    }

    private SSLSession createSSLSession(CassandraServer cassandraServer) throws SSLPeerUnverifiedException {
        return createSSLSession(cassandraServer.cassandraHostName(), Optional.empty());
    }

    private SSLSession createSSLSession(String cn, Optional<String> maybeIpAddress) throws SSLPeerUnverifiedException {
        SSLSession sslSession = mock(SSLSession.class);
        X509Certificate certificate = mock(X509Certificate.class);
        X500Principal principal = mock(X500Principal.class);
        when(principal.toString()).thenReturn("CN=" + cn);
        maybeIpAddress.ifPresent(ipAddress -> {
            try {
                when(certificate.getSubjectAlternativeNames())
                        .thenReturn(ImmutableList.of(ImmutableList.of(7, ipAddress)));
            } catch (CertificateParsingException e) {
                throw new RuntimeException(e);
            }
        });
        when(certificate.getSubjectX500Principal()).thenReturn(principal);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[] {certificate});
        return sslSession;
    }
}
