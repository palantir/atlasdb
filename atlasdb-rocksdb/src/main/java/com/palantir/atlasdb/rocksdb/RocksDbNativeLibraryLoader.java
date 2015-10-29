/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.rocksdb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.rocksdb.NativeLibraryLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class RocksDbNativeLibraryLoader {
    private static final Logger log = LoggerFactory.getLogger(RocksDbNativeLibraryLoader.class);
    private static AtomicReference<String> staticTmpDir = new AtomicReference<>();

    public static void load(File tmpDir) {
        String path = tmpDir.getAbsolutePath();
        if (staticTmpDir.compareAndSet(null, path)) {
            try {
                NativeLibraryLoader.getInstance().loadLibrary(tmpDir.getAbsolutePath());
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        } else if (!staticTmpDir.get().equals(path)) {
            log.error("Cannot load native rocksdb libraries to {}, " +
                    "native libraries were already loaded to {}", path, staticTmpDir.get());
        }
    }
}
