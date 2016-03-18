/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 *
 * THIS SOFTWARE CONTAINS PROPRIETARY AND CONFIDENTIAL INFORMATION OWNED BY PALANTIR TECHNOLOGIES INC.
 * UNAUTHORIZED DISCLOSURE TO ANY THIRD PARTY IS STRICTLY PROHIBITED
 *
 * For good and valuable consideration, the receipt and adequacy of which is acknowledged by Palantir and recipient
 * of this file ("Recipient"), the parties agree as follows:
 *
 * This file is being provided subject to the non-disclosure terms by and between Palantir and the Recipient.
 *
 * Palantir solely shall own and hereby retains all rights, title and interest in and to this software (including,
 * without limitation, all patent, copyright, trademark, trade secret and other intellectual property rights) and
 * all copies, modifications and derivative works thereof.  Recipient shall and hereby does irrevocably transfer and
 * assign to Palantir all right, title and interest it may have in the foregoing to Palantir and Palantir hereby
 * accepts such transfer. In using this software, Recipient acknowledges that no ownership rights are being conveyed
 * to Recipient.  This software shall only be used in conjunction with properly licensed Palantir products or
 * services.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.palantir.atlasdb.keyvalue.impl;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public class TableSplittingKeyValueServiceTest {
    private static final String TABLE = "namespace.table";
    private static final String NAMESPACE = "namespace";
    private static final Cell CELL = Cell.create("row".getBytes(), "column".getBytes());
    private static final byte[] VALUE = "value".getBytes();
    private static final long TIMESTAMP = 123l;
    public static final ImmutableMap<Cell, byte[]> VALUES = ImmutableMap.of(CELL, VALUE);

    private final Mockery mockery = new Mockery();
    private final KeyValueService kvs = mockery.mock(KeyValueService.class, "kvs");
    private final KeyValueService otherKvs = mockery.mock(KeyValueService.class, "otherKvs");

    @Test
    public void delegatesMethodsToTheKvsAssociatedWithTheTable() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(otherKvs, kvs),
                ImmutableMap.of(TABLE, kvs)
        );

        mockery.checking(new Expectations() {{
            oneOf(kvs).put(TABLE, VALUES, TIMESTAMP);
        }});

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void delegatesMethodsToTheKvsAssociatedWithTheNamespaceIfNoTableMappingExists() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(otherKvs, kvs),
                ImmutableMap.<String, KeyValueService>of(),
                ImmutableMap.of(NAMESPACE, kvs)
        );

        mockery.checking(new Expectations() {{
            oneOf(kvs).put(TABLE, VALUES, TIMESTAMP);
        }});

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void prioritisesTableDelegatesOverNamespaceDelegates() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(otherKvs, kvs),
                ImmutableMap.of(TABLE, kvs),
                ImmutableMap.of(NAMESPACE, otherKvs)
        );

        mockery.checking(new Expectations() {{
            oneOf(kvs).put(TABLE, VALUES, TIMESTAMP);
        }});

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void defaultsToTheFirstKvsInTheListIfNoMappingsMatch() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(otherKvs, kvs),
                ImmutableMap.of("not-this", kvs)
        );

        mockery.checking(new Expectations() {{
            oneOf(otherKvs).put(TABLE, VALUES, TIMESTAMP);
        }});

        splittingKvs.put(TABLE, VALUES, TIMESTAMP);
    }

    @Test
    public void splitsTableMetadataIntoTheCorrectTables() {
        TableSplittingKeyValueService splittingKvs = TableSplittingKeyValueService.create(
                ImmutableList.of(otherKvs, kvs),
                ImmutableMap.of(
                        "table1", kvs,
                        "table2", otherKvs,
                        "table3", otherKvs)
        );

        final byte[] metadata1 = "1".getBytes();
        final byte[] metadata2 = "2".getBytes();
        final byte[] metadata3 = "3".getBytes();

        mockery.checking(new Expectations() {{
            oneOf(kvs).createTables(ImmutableMap.of("table1", metadata1));
            oneOf(otherKvs).createTables(ImmutableMap.of(
                    "table2", metadata2,
                    "table3", metadata3
            ));
        }});

        splittingKvs.createTables(ImmutableMap.of(
                "table1", metadata1,
                "table2", metadata2,
                "table3", metadata3
        ));
    }
}
