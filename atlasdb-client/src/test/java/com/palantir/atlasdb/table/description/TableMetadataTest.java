/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.table.description;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class TableMetadataTest {
    private static final NameMetadataDescription NAME_METADATA_DESCRIPTION = new NameMetadataDescription();
    private static final ColumnMetadataDescription COLUMN_METADATA_DESCRIPTION = new ColumnMetadataDescription();
    private static final ConflictHandler CONFLICT_HANDLER = ConflictHandler.RETRY_ON_WRITE_WRITE;

    private static final TableMetadata DEFAULT_TABLE_METADATA = new TableMetadata();
    private static final TableMetadata LIGHTLY_SPECIFIED_TABLE_METADATA = new TableMetadata(
            NAME_METADATA_DESCRIPTION,
            COLUMN_METADATA_DESCRIPTION,
            CONFLICT_HANDLER);
    private static final TableMetadata LOGGABILITY_NOT_SPECIFIED_TABLE_METADATA = new TableMetadata(
            NAME_METADATA_DESCRIPTION,
            COLUMN_METADATA_DESCRIPTION,
            CONFLICT_HANDLER,
            TableMetadataPersistence.CachePriority.WARM,
            TableMetadataPersistence.PartitionStrategy.ORDERED,
            false,
            0,
            false,
            TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
            TableMetadataPersistence.ExpirationStrategy.NEVER,
            false);

    private static final TableMetadata NAME_LOGGABLE_TABLE_METADATA = createWithSpecifiedLoggability(true);
    private static final TableMetadata NAME_NOT_LOGGABLE_TABLE_METADATA = createWithSpecifiedLoggability(false);

    @Test
    public void nameIsNotLoggableByDefault() {
        assertThat(DEFAULT_TABLE_METADATA.isNameLoggable()).isFalse();
    }

    @Test
    public void nameIsNotLoggableIfMetadataIsOnlyLightlySpecified() {
        assertThat(LIGHTLY_SPECIFIED_TABLE_METADATA.isNameLoggable()).isFalse();
    }

    @Test
    public void nameIsNotLoggableIfNotSpecified() {
        assertThat(LOGGABILITY_NOT_SPECIFIED_TABLE_METADATA.isNameLoggable()).isFalse();
    }

    @Test
    public void nameCanBeSpecifiedToBeLoggable() {
        assertThat(NAME_LOGGABLE_TABLE_METADATA.isNameLoggable()).isTrue();
    }

    @Test
    public void nameCanBeSpecifiedToBeNotLoggable() {
        assertThat(NAME_NOT_LOGGABLE_TABLE_METADATA.isNameLoggable()).isFalse();
    }

    @Test
    public void canSerializeAndDeserializeKeepingLoggability() {
        TableMetadataPersistence.TableMetadata.Builder builder = NAME_LOGGABLE_TABLE_METADATA.persistToProto();
        assertThat(TableMetadata.hydrateFromProto(builder.build()))
                .isEqualTo(NAME_LOGGABLE_TABLE_METADATA)
                .matches(TableMetadata::isNameLoggable);
    }

    @Test
    public void canSerializeAndDeserializeKeepingNonLoggability() {
        TableMetadataPersistence.TableMetadata.Builder builder =
                NAME_NOT_LOGGABLE_TABLE_METADATA.persistToProto();
        assertThat(TableMetadata.hydrateFromProto(builder.build()))
                .isEqualTo(NAME_NOT_LOGGABLE_TABLE_METADATA)
                .matches(tableMetadata -> !tableMetadata.isNameLoggable());
    }

    private static TableMetadata createWithSpecifiedLoggability(boolean loggable) {
        return new TableMetadata(
                NAME_METADATA_DESCRIPTION,
                COLUMN_METADATA_DESCRIPTION,
                CONFLICT_HANDLER,
                TableMetadataPersistence.CachePriority.WARM,
                TableMetadataPersistence.PartitionStrategy.ORDERED,
                false,
                0,
                false,
                TableMetadataPersistence.SweepStrategy.CONSERVATIVE,
                TableMetadataPersistence.ExpirationStrategy.NEVER,
                false,
                loggable);
    }
}
