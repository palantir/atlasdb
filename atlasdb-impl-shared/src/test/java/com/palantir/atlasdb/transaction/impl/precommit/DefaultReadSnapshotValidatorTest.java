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

package com.palantir.atlasdb.transaction.impl.precommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.SweepStrategy;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.api.precommit.PreCommitRequirementValidator;
import com.palantir.atlasdb.transaction.api.precommit.ReadSnapshotValidator;
import com.palantir.atlasdb.transaction.api.precommit.ReadSnapshotValidator.ValidationState;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultReadSnapshotValidatorTest {
    private static final long TIMESTAMP = 888L;
    private static final TableReference CONSERVATIVE_TABLE =
            TableReference.createFromFullyQualifiedName("conservative.table");
    private static final TableReference THOROUGH_TABLE = TableReference.createFromFullyQualifiedName("thorough.table");

    private static final Map<TableReference, SweepStrategy> SWEEP_STRATEGY_MAP = ImmutableMap.of(
            CONSERVATIVE_TABLE, SweepStrategy.CONSERVATIVE,
            THOROUGH_TABLE, SweepStrategy.THOROUGH);

    private static final SweepStrategyManager FAKE_SWEEP_STRATEGY_MANAGER = tableRef -> {
        SweepStrategy strategyForTable = SWEEP_STRATEGY_MAP.get(tableRef);
        if (strategyForTable == null) {
            throw new SafeIllegalArgumentException(
                    "No sweep strategy found for table", SafeArg.of("tableRef", tableRef));
        }
        return strategyForTable;
    };

    @Mock
    private PreCommitRequirementValidator preCommitRequirementValidator;

    private ReadSnapshotValidator readSnapshotValidator;

    @BeforeEach
    public void setUp() {
        readSnapshotValidator = new DefaultReadSnapshotValidator(
                preCommitRequirementValidator,
                true,
                FAKE_SWEEP_STRATEGY_MANAGER,
                () -> ImmutableTransactionConfig.builder().build());
    }

    @Test
    public void conservativeTablesDoNotRequirePreCommitValidation() {
        assertThat(readSnapshotValidator.doesTableRequirePreCommitValidation(CONSERVATIVE_TABLE, false))
                .as("validation is not needed for a conservative table (with incomplete reads)")
                .isFalse();
        assertThat(readSnapshotValidator.doesTableRequirePreCommitValidation(CONSERVATIVE_TABLE, true))
                .as("validation is not needed for a conservative table (with complete reads)")
                .isFalse();
    }

    @Test
    public void thoroughTablesRequirePreCommitValidationIfAndOnlyIfIncompleteReadsWerePerformed() {
        assertThat(readSnapshotValidator.doesTableRequirePreCommitValidation(THOROUGH_TABLE, false))
                .as("validation is needed for a thorough table (with incomplete reads)")
                .isTrue();
        assertThat(readSnapshotValidator.doesTableRequirePreCommitValidation(THOROUGH_TABLE, true))
                .as("validation is not needed for a thorough table (with complete reads)")
                .isFalse();
    }

    @MethodSource("testTables")
    @ParameterizedTest
    public void tablesAlwaysRequirePreCommitValidationIfExplicitlyInstructedToLockImmutableTimestamp(
            TableReference testTable) {
        readSnapshotValidator = new DefaultReadSnapshotValidator(
                preCommitRequirementValidator,
                true,
                FAKE_SWEEP_STRATEGY_MANAGER,
                () -> ImmutableTransactionConfig.builder()
                        .lockImmutableTsOnReadOnlyTransactions(true)
                        .build());
        assertThat(readSnapshotValidator.doesTableRequirePreCommitValidation(testTable, false))
                .as("validation is needed (with incomplete reads), if instructed to lock the immutable timestamp")
                .isTrue();
        assertThat(readSnapshotValidator.doesTableRequirePreCommitValidation(testTable, true))
                .as("validation is needed (with complete reads), if instructed to lock the immutable timestamp")
                .isTrue();
    }

    @MethodSource("testTables")
    @ParameterizedTest
    public void incompleteReadsAreNotCompletelyValidatedAndDoNotCheckPreCommitRequirementsIfConfiguredNotTo(
            TableReference testTable) {
        readSnapshotValidator = new DefaultReadSnapshotValidator(
                preCommitRequirementValidator,
                false,
                FAKE_SWEEP_STRATEGY_MANAGER,
                () -> ImmutableTransactionConfig.builder()
                        .lockImmutableTsOnReadOnlyTransactions(true)
                        .build());

        assertThat(readSnapshotValidator.throwIfPreCommitRequirementsNotMetOnRead(testTable, TIMESTAMP, false))
                .isEqualTo(ValidationState.NOT_COMPLETELY_VALIDATED);
        verify(preCommitRequirementValidator, never()).throwIfPreCommitRequirementsNotMet(anyLong());
    }

    @MethodSource("testTables")
    @ParameterizedTest
    public void completeReadsAreCompletelyValidatedAndDoNotCheckPreCommitRequirementsIfConfiguredNotTo(
            TableReference testTable) {
        readSnapshotValidator = new DefaultReadSnapshotValidator(
                preCommitRequirementValidator,
                false,
                FAKE_SWEEP_STRATEGY_MANAGER,
                () -> ImmutableTransactionConfig.builder()
                        .lockImmutableTsOnReadOnlyTransactions(true)
                        .build());

        assertThat(readSnapshotValidator.throwIfPreCommitRequirementsNotMetOnRead(testTable, TIMESTAMP, true))
                .isEqualTo(ValidationState.COMPLETELY_VALIDATED);
        verify(preCommitRequirementValidator, never()).throwIfPreCommitRequirementsNotMet(anyLong());
    }

    @Test
    public void incompleteReadsOnConservativeTablesDoNotRequireValidationOnReadByDefault() {
        assertThat(readSnapshotValidator.throwIfPreCommitRequirementsNotMetOnRead(CONSERVATIVE_TABLE, TIMESTAMP, false))
                .isEqualTo(ValidationState.NOT_COMPLETELY_VALIDATED);
        verify(preCommitRequirementValidator, never()).throwIfPreCommitRequirementsNotMet(anyLong());
    }

    @Test
    public void incompleteReadsOnThoroughTablesRequireValidationOnReadByDefault() {
        assertThat(readSnapshotValidator.throwIfPreCommitRequirementsNotMetOnRead(THOROUGH_TABLE, TIMESTAMP, false))
                .isEqualTo(ValidationState.COMPLETELY_VALIDATED);
        verify(preCommitRequirementValidator).throwIfPreCommitRequirementsNotMet(anyLong());
    }

    @MethodSource("testTables")
    @ParameterizedTest
    public void completeReadsAreCompletelyValidatedAndDoNotRequireValidationOnReadByDefault(TableReference testTable) {
        assertThat(readSnapshotValidator.throwIfPreCommitRequirementsNotMetOnRead(testTable, TIMESTAMP, true))
                .isEqualTo(ValidationState.COMPLETELY_VALIDATED);
        verify(preCommitRequirementValidator, never()).throwIfPreCommitRequirementsNotMet(anyLong());
    }

    private static Stream<TableReference> testTables() {
        return Stream.of(CONSERVATIVE_TABLE, THOROUGH_TABLE);
    }
}
