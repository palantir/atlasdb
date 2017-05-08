/**
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
package com.palantir.lock.logger;

import java.io.File;
import java.io.IOException;

public class LockServiceLoggerTestUtils {
    public static final String TEST_LOG_STATE_DIR = "log-state";

    public static void cleanUpLogStateDir() throws IOException {
        File rootDir = new File(TEST_LOG_STATE_DIR);
        if (rootDir.isDirectory()) {
            for (File file : rootDir.listFiles()) {
                file.delete();
            }
        }
        rootDir.delete();
    }
}
