/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClient;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.pooling.PoolingContainer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HostnamesByIpSupplierTest {
    @Mock
    CassandraClient client;

    private final Supplier<Map<String, String>> hostnamesByIpSupplier =
            new HostnamesByIpSupplier(() -> new DummyClientPool(client));

    @Test
    public void keyspaceNotAccessibleDoesNotError() throws Exception {
        when(client.describe_keyspace("system_palantir")).thenThrow(new NotFoundException());

        assertThatCode(hostnamesByIpSupplier::get).doesNotThrowAnyException();
        assertThat(hostnamesByIpSupplier.get()).isEmpty();
    }

    @Test
    public void tableNotAccessibleDoesNotError() throws Exception {
        when(client.describe_keyspace("system_palantir"))
                .thenReturn(new KsDef("system_palantir", "", ImmutableList.of()));

        assertThatCode(hostnamesByIpSupplier::get).doesNotThrowAnyException();
        assertThat(hostnamesByIpSupplier.get()).isEmpty();
    }

    @Test
    public void unexpectedTableFormatDoesNotError() throws Exception {
        setupKeyspaceAndTable();
        CqlResult cqlResult = createMockCqlResult(ImmutableList.of(ImmutableList.of(
                createColumn("unknown_name", PtBytes.toBytes("unknown_value")),
                createColumn("another_unknown_name", PtBytes.toBytes("another_unknown_value")))));
        when(client.execute_cql3_query(any(), any(), any())).thenReturn(cqlResult);

        assertThat(hostnamesByIpSupplier.get()).isEmpty();
        verify(client).execute_cql3_query(any(), any(), any());
    }

    @Test
    public void hostnamesByIpAreReturnedWhenPresent() throws Exception {
        setupKeyspaceAndTable();
        CqlResult cqlResult = createMockCqlResult(ImmutableList.of(
                ImmutableList.of(
                        createColumn("ip", PtBytes.toBytes("10.0.0.0")),
                        createColumn("hostname", PtBytes.toBytes("cassandra-1"))),
                ImmutableList.of(
                        createColumn("ip", PtBytes.toBytes("10.0.0.1")),
                        createColumn("hostname", PtBytes.toBytes("cassandra-2")))));
        when(client.execute_cql3_query(any(), any(), any())).thenReturn(cqlResult);

        Map<String, String> hostnamesByIp = hostnamesByIpSupplier.get();
        assertThat(hostnamesByIp).containsEntry("10.0.0.0", "cassandra-1");
        assertThat(hostnamesByIp).containsEntry("10.0.0.1", "cassandra-2");
    }

    @Test
    public void unknownColumnsAreIgnored() throws Exception {
        setupKeyspaceAndTable();
        CqlResult cqlResult = createMockCqlResult(ImmutableList.of(ImmutableList.of(
                createColumn("ip", PtBytes.toBytes("10.0.0.0")),
                createColumn("hostname", PtBytes.toBytes("cassandra-1")),
                createColumn("unknown_column", PtBytes.toBytes("unknown_column_value")))));
        when(client.execute_cql3_query(any(), any(), any())).thenReturn(cqlResult);

        Map<String, String> hostnamesByIp = hostnamesByIpSupplier.get();
        assertThat(hostnamesByIp).containsEntry("10.0.0.0", "cassandra-1");
    }

    private void setupKeyspaceAndTable() throws TException {
        when(client.describe_keyspace("system_palantir"))
                .thenReturn(new KsDef(
                        "system_palantir", "", ImmutableList.of(new CfDef("system_palantir", "hostnames_by_ip"))));
    }

    private static CqlResult createMockCqlResult(List<List<Column>> rowsAndCols) {
        CqlResult result = mock(CqlResult.class);
        List<CqlRow> rows = new ArrayList<>();
        for (List<Column> cols : rowsAndCols) {
            CqlRow row = mock(CqlRow.class);
            when(row.getColumns()).thenReturn(cols);
            rows.add(row);
        }
        when(result.getRows()).thenReturn(rows);
        return result;
    }

    private static Column createColumn(String name, byte[] value) {
        Column column = new Column();
        column.setName(PtBytes.toBytes(name));
        column.setValue(value);
        return column;
    }

    private static class DummyClientPool implements PoolingContainer<CassandraClient> {
        private final CassandraClient client;

        DummyClientPool(CassandraClient client) {
            this.client = client;
        }

        @Override
        public <V, K extends Exception> V runWithPooledResource(FunctionCheckedException<CassandraClient, V, K> fn)
                throws K {
            return fn.apply(client);
        }

        @Override
        public <V> V runWithPooledResource(Function<CassandraClient, V> fn) {
            return fn.apply(client);
        }

        @Override
        public void shutdownPooling() {}
    }
}
