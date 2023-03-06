package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Test;

public final class CassandraServerOriginTest {

    private static final CassandraServer SERVER_1 = createCassandraServer("serverOne");
    private static final CassandraServer SERVER_2 = createCassandraServer("serverTwo");
    private static final CassandraServer SERVER_3 = createCassandraServer("serverThree");

    @Test
    public void mapAllServersToOriginFromServerStreamDeduplicatesInput() {
        CassandraServer cloneOfServer1 = createCassandraServer(SERVER_1.cassandraHostName());
        CassandraServerOrigin origin = CassandraServerOrigin.CONFIG;
        assertThat(CassandraServerOrigin.mapAllServersToOrigin(
                        Stream.of(SERVER_1, SERVER_2, SERVER_3, cloneOfServer1, SERVER_2, SERVER_3, SERVER_2), origin))
                .containsExactlyInAnyOrderEntriesOf(Map.of(SERVER_1, origin, SERVER_2, origin, SERVER_3, origin));
    }

    @Test
    public void mapAllServersToOriginFromServerSetReturnsMapWithAllSetElementsAsKeysAndAUniqueValue() {
        CassandraServerOrigin origin = CassandraServerOrigin.TOKEN_RANGE;
        assertThat(CassandraServerOrigin.mapAllServersToOrigin(Set.of(SERVER_1, SERVER_2, SERVER_3), origin))
                .containsExactlyInAnyOrderEntriesOf(Map.of(SERVER_1, origin, SERVER_2, origin, SERVER_3, origin));
    }

    @Test
    public void mapAllServersToOriginFromServerStreamWithUniqueElementsReturnsMapWithAllElementsAndAUniqueValue() {
        CassandraServerOrigin origin = CassandraServerOrigin.LAST_KNOWN;
        assertThat(CassandraServerOrigin.mapAllServersToOrigin(Set.of(SERVER_1, SERVER_2, SERVER_3), origin))
                .containsExactlyInAnyOrderEntriesOf(Map.of(SERVER_1, origin, SERVER_2, origin, SERVER_3, origin));
    }

    @Test
    public void mapAllServersToOriginFromServerStreamReturnsEmptyMapOnEmptyStream() {
        assertThat(CassandraServerOrigin.mapAllServersToOrigin(Stream.empty(), CassandraServerOrigin.CONFIG))
                .isEmpty();
    }

    @Test
    public void mapAllServersToOriginFromSetStreamReturnsEmptyMapOnEmptySet() {
        assertThat(CassandraServerOrigin.mapAllServersToOrigin(Set.of(), CassandraServerOrigin.LAST_KNOWN))
                .isEmpty();
    }

    private static CassandraServer createCassandraServer(String hostname) {
        return CassandraServer.of(hostname, InetSocketAddress.createUnresolved(hostname, 88));
    }
}