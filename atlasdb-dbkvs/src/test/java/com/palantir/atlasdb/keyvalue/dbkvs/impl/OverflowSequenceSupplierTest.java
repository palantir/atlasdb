/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle.OverflowSequenceSupplier;
import com.palantir.nexus.db.sql.SqlConnection;
import org.junit.Test;

public class OverflowSequenceSupplierTest {

    @Test
    public void shouldGetConsecutiveOverflowIdsFromSameSupplier() {
        final ConnectionSupplier conns = mock(ConnectionSupplier.class);
        final SqlConnection sqlConnection = mock(SqlConnection.class);

        when(conns.get()).thenReturn(sqlConnection);
        when(sqlConnection.selectLongUnregisteredQuery(anyString())).thenReturn(1L);

        OverflowSequenceSupplier sequenceSupplier = OverflowSequenceSupplier.create(conns, "a_");
        long firstSequenceId = sequenceSupplier.get();
        long nextSequenceId = sequenceSupplier.get();

        assertThat(nextSequenceId - firstSequenceId).isEqualTo(1L);
    }

    @Test
    public void shouldNotGetOverflowIdsWithOverlappingCachesFromDifferentSuppliers() {
        final ConnectionSupplier conns = mock(ConnectionSupplier.class);
        final SqlConnection sqlConnection = mock(SqlConnection.class);

        when(conns.get()).thenReturn(sqlConnection);
        when(sqlConnection.selectLongUnregisteredQuery(anyString())).thenReturn(1L, 1001L);

        long firstSequenceId = OverflowSequenceSupplier.create(conns, "a_").get();
        long secondSequenceId = OverflowSequenceSupplier.create(conns, "a_").get();

        assertThat(secondSequenceId - firstSequenceId).isGreaterThanOrEqualTo(1000L);
    }

    @Test
    public void shouldSkipValuesReservedByOtherSupplier() {
        final ConnectionSupplier conns = mock(ConnectionSupplier.class);
        final SqlConnection sqlConnection = mock(SqlConnection.class);

        when(conns.get()).thenReturn(sqlConnection);
        when(sqlConnection.selectLongUnregisteredQuery(anyString())).thenReturn(1L, 1001L, 2001L);

        OverflowSequenceSupplier firstSupplier = OverflowSequenceSupplier.create(conns, "a_");
        firstSupplier.get(); // gets 1
        OverflowSequenceSupplier.create(conns, "a_").get(); // gets 1001

        // After 1000 gets from the first supplier, we should get to 1000
        long id = 0;
        for (int i = 0; i < 999; i++) {
            id = firstSupplier.get();
        }
        assertThat(id).isEqualTo(1000L);

        // Should then skip to 2001
        assertThat(firstSupplier.get()).isEqualTo(2001L);
    }
}
