/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampRangeDelete;
import com.palantir.atlasdb.keyvalue.api.WriteReference;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.sweep.Sweeper;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
// TODO(mdaudali): We should test this class properly. I added a test to catch a specific regression, not intending
//  to test the whole class at the time of writing.
public final class SweepQueueDeleterTest {
    private final Function<TableReference, Optional<LogSafety>> tablesToTrackDeletions = _1 -> Optional.empty();

    @Mock
    private KeyValueService keyValueService;

    @Mock
    private TargetedSweepFollower targetedSweepFollower;

    @Mock
    private TargetedSweepFilter targetedSweepFilter;

    private SweepQueueDeleter sweepQueueDeleter;

    @BeforeEach
    public void setUp() {
        sweepQueueDeleter = new SweepQueueDeleter(
                keyValueService, targetedSweepFollower, targetedSweepFilter, tablesToTrackDeletions);
    }

    @Test
    public void propagatesDeleteAllTimestampsException() {
        TableReference tableReference = TableReference.createFromFullyQualifiedName("test.table");
        when(keyValueService.getAllTableNames()).thenReturn(Set.of(tableReference));

        Cell cell = Cell.create(new byte[] {1}, new byte[] {1});
        Collection<WriteInfo> writeInfos = List.of(WriteInfo.of(WriteReference.write(tableReference, cell), 10));
        when(targetedSweepFilter.filter(writeInfos)).thenReturn(writeInfos);

        RuntimeException exception = new RuntimeException("exception");
        doThrow(exception)
                .when(keyValueService)
                .deleteAllTimestamps(
                        tableReference,
                        Map.of(
                                cell,
                                new TimestampRangeDelete.Builder()
                                        .timestamp(10)
                                        .deleteSentinels(true)
                                        .endInclusive(false)
                                        .build()));

        assertThatThrownBy(() -> sweepQueueDeleter.sweep(writeInfos, Sweeper.THOROUGH))
                .isSameAs(exception);
    }
}
