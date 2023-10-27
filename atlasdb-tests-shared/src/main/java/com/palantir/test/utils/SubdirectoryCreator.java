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

public final class SubdirectoryCreator {

    private SubdirectoryCreator() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }

    public static File getAndCreateSubdirectory(File base, String subdirectoryName) {
        File file = base.toPath().resolve(subdirectoryName).toFile();
        if (file.mkdirs()) {
            return file;
        }
        throw new SafeRuntimeException("Unexpected error when creating a subdirectory");
    }
}
