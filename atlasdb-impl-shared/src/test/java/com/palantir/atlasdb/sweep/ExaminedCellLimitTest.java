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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.api.Cell;
import org.junit.Test;

public class ExaminedCellLimitTest {
    private String startingRow;
    private int maxCellsToExamine;
    private int cellsExamined;
    private String currentRow;

    @Test
    public void whenSameRowAndLessThanLimit_thenExaminedEnoughIsFalse() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(1);
        on("row1");

        // Then
        examinedEnoughCellsIs(false);
    }

    @Test
    public void whenSameRowAndAtLimit_thenExaminedEnoughIsFalse() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(10);
        on("row1");

        // Then
        examinedEnoughCellsIs(false);
    }

    @Test
    public void whenSameRowAndExceededLimit_thenExaminedEnoughIsFalse() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(11);
        on("row1");

        // Then
        examinedEnoughCellsIs(false);
    }

    @Test
    public void whenSameRowAndGreatlyExceededLimit_thenExaminedEnoughIsFalse() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(1000);
        on("row1");

        // Then
        examinedEnoughCellsIs(false);
    }

    @Test
    public void whenNextRowAndLessThanLimit_thenExaminedEnoughIsFalse() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(1);
        on("row2");

        // Then
        examinedEnoughCellsIs(false);
    }

    @Test
    public void whenNextRowAndAtLimit_thenExaminedEnoughIsTrue() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(10);
        on("row2");

        // Then
        examinedEnoughCellsIs(true);
    }

    @Test
    public void whenNextRowAndGreaterThanLimit_thenExaminedEnoughIsTrue() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(11);
        on("row2");

        // Then
        examinedEnoughCellsIs(true);
    }

    @Test
    public void whenNextRowAndGreatlyExceededLimit_thenExaminedEnoughIsTrue() {
        // Given
        startingOn("row1");
        maxCellsToExamineIs(10);

        // When
        examined(1000);
        on("row2");

        // Then
        examinedEnoughCellsIs(true);
    }

    // Given
    private void startingOn(String rowName) {
        startingRow = rowName;
    }

    private void maxCellsToExamineIs(int maxCells) {
        maxCellsToExamine = maxCells;
    }

    // When
    private void examined(int examined) {
        cellsExamined = examined;
    }

    private void on(String curRow) {
        currentRow = curRow;
    }

    // Then

    private void examinedEnoughCellsIs(boolean expected) {
        CellsToSweepPartitioningIterator.ExaminedCellLimit limit =
                new CellsToSweepPartitioningIterator.ExaminedCellLimit(startingRow.getBytes(), maxCellsToExamine);

        assertThat(limit.examinedEnoughCells(cellsExamined, cell())).isEqualTo(expected);
    }

    private Cell cell() {
        return Cell.create(currentRow.getBytes(), "COLUMN".getBytes());
    }
}
