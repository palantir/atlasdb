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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.commons.io.FileUtils;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class TemporaryConfigurationHolder extends ExternalResource {
    @VisibleForTesting
    static final String TEMP_DATA_DIR = "<TEMP_DATA_DIR>";

    private final TemporaryFolder temporaryFolder;
    private final File configTemplate;

    private File temporaryConfigFile;
    private File temporaryLogDirectory;

    TemporaryConfigurationHolder(TemporaryFolder temporaryFolder, File configTemplate) {
        this.temporaryFolder = temporaryFolder;
        this.configTemplate = configTemplate;
    }

    @Override
    public void before() throws Exception {
        temporaryConfigFile = temporaryFolder.newFile();
        temporaryLogDirectory = temporaryFolder.newFolder();
        createTemporaryConfigFile();
    }

    private void createTemporaryConfigFile() throws IOException {
        writeNewFileWithPlaceholderSubstituted(configTemplate, temporaryLogDirectory.getPath(), temporaryConfigFile);
    }

    @VisibleForTesting
    static void writeNewFileWithPlaceholderSubstituted(File sourceFile, String substitution, File destinationFile)
            throws IOException {
        Preconditions.checkArgument(!sourceFile.getCanonicalFile().equals(destinationFile.getCanonicalFile()),
                "The source and destination files both point to '%s'.", sourceFile.getCanonicalPath());

        String oldConfig = FileUtils.readFileToString(sourceFile, StandardCharsets.UTF_8);
        String newConfig = replaceTempDataDirPlaceholder(oldConfig, substitution);
        FileUtils.writeStringToFile(destinationFile, newConfig, StandardCharsets.UTF_8);
    }

    @VisibleForTesting
    static String replaceTempDataDirPlaceholder(String config, String substitution) {
        return config.replace(TEMP_DATA_DIR, substitution);
    }

    String getTemporaryConfigFileLocation() {
        return temporaryConfigFile.getPath();
    }
}
