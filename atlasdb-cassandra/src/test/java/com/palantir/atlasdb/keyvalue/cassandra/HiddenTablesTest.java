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
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import org.junit.Test;

public class HiddenTablesTest {
    @Test
    public void shouldSayTimestampIsHidden() {
        assertThat(HiddenTables.isHidden(AtlasDbConstants.TIMESTAMP_TABLE), is(true));
    }

    @Test
    public void shouldSayMetadataIsHidden() {
        assertThat(HiddenTables.isHidden(AtlasDbConstants.DEFAULT_METADATA_TABLE), is(true));
    }

    @Test
    public void shouldSayAnOldStyleLocksTableIsHidden() {
        assertThat(HiddenTables.isHidden(TableReference.createWithEmptyNamespace("_locks")), is(true));
    }

    @Test
    public void shouldSayANewStyleLocksTableIsHidden() {
        assertThat(HiddenTables.isHidden(TableReference.createWithEmptyNamespace("_locks_aaaa_123")), is(true));
    }

    @Test
    public void shouldSayANamespacedTableIsNotHidden() {
        assertThat(HiddenTables.isHidden(TableReference.createFromFullyQualifiedName("namespace.table")), is(false));
    }

    @Test
    public void shouldSayANonNamespacedVisibleTableIsNotHidden() {
        assertThat(HiddenTables.isHidden(TableReference.createWithEmptyNamespace("table")), is(false));
    }

    @Test
    public void shouldSayPersistedLocksTableIsHidden() {
        assertThat(HiddenTables.isHidden(AtlasDbConstants.PERSISTED_LOCKS_TABLE), is(true));
    }
}
