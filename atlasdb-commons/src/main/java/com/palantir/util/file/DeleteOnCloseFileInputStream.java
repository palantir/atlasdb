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
package com.palantir.util.file;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteOnCloseFileInputStream extends FileInputStream {
    private static final Logger log = LoggerFactory.getLogger(DeleteOnCloseFileInputStream.class);

    private File file;
    private boolean closed = false;

    public DeleteOnCloseFileInputStream(File file) throws FileNotFoundException {
        super(file);
        this.file = file;
    }

    public File getFile() {
        return file;
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            if (!closed) {
                if (!file.delete()) {
                    log.warn("Failed to delete file {}", file.getAbsolutePath());
                } else if (log.isDebugEnabled()) {
                    log.debug("Successfully deleted file {}", file.getAbsolutePath());
                }
                closed = true;
            }
        }
    }

}
