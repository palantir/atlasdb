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
package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TemporaryConfigurationHolderTest {
    private static final String TEMP_DATA_DIR = TemporaryConfigurationHolder.TEMP_DATA_DIR;
    private static final String PREFIX = "_";
    private static final String TEST_FILE_DATA = PREFIX + TEMP_DATA_DIR;
    private static final String SUBSTITUTION = "sub";

    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Test
    public void canReplaceTempDataDirWithActualDirectory() {
        String substituted = TemporaryConfigurationHolder.replaceTempDataDirPlaceholder(TEMP_DATA_DIR, SUBSTITUTION);
        assertEquals(SUBSTITUTION, substituted);
    }

    @Test
    public void canHandlePotentiallyRecursiveSubstitution() {
        String substituted = TemporaryConfigurationHolder.replaceTempDataDirPlaceholder(TEMP_DATA_DIR, TEMP_DATA_DIR);
        assertEquals(TEMP_DATA_DIR, substituted);
    }

    @Test
    public void canWriteToNewFile() throws IOException {
        File source = TEMPORARY_FOLDER.newFile();
        FileUtils.writeStringToFile(source, TEST_FILE_DATA);
        File destination = TEMPORARY_FOLDER.newFile();
        TemporaryConfigurationHolder.writeNewFileWithPlaceholderSubstituted(source, SUBSTITUTION, destination);

        String destinationFileContent = FileUtils.readFileToString(destination);
        assertEquals(PREFIX + SUBSTITUTION, destinationFileContent);
    }

    @Test
    public void throwsIfTryingToOverwriteSourceFile() throws IOException {
        File source = TEMPORARY_FOLDER.newFile();
        FileUtils.writeStringToFile(source, TEST_FILE_DATA);
        File destination = new File(source.getPath());

        assertThatThrownBy(() ->
                TemporaryConfigurationHolder.writeNewFileWithPlaceholderSubstituted(source, "", destination))
                .isInstanceOf(IllegalArgumentException.class);

        String sourceFileContent = FileUtils.readFileToString(source);
        assertEquals(TEST_FILE_DATA, sourceFileContent);
    }
}
