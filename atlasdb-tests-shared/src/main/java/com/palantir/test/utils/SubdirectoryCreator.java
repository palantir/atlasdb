/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.test.utils;

import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.File;
import java.io.IOException;

public final class SubdirectoryCreator {

    private SubdirectoryCreator() {}

    public static File createAndGetSubdirectory(File parentDirectory, String subdirectoryName) {
        File subdirectory = parentDirectory.toPath().resolve(subdirectoryName).toFile();
        if (subdirectory.mkdirs()) {
            return subdirectory;
        }
        throw new SafeRuntimeException("Unexpected error when creating a subdirectory");
    }

    public static File createAndGetFile(File directory, String fileName) throws IOException {
        File file = directory.toPath().resolve(fileName).toFile();
        if (file.createNewFile()) {
            return file;
        }
        throw new SafeRuntimeException("Unexpected error when creating a file");
    }
}
