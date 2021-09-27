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

import com.google.common.io.BaseEncoding;
import com.palantir.logsafe.exceptions.SafeIoException;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public final class TempFileUtils {
    private TempFileUtils() {
        /**/
    }

    private static final Random tempFileRandom = new Random();

    private static byte[] generateRandomBytes(int n) {
        byte[] bytes = new byte[n];
        synchronized (tempFileRandom) {
            tempFileRandom.nextBytes(bytes);
        }
        return bytes;
    }

    public static File createTempFile(String prefix, String suffix) throws IOException {
        ensureTempDirectoryExists();
        byte[] bytes = generateRandomBytes(32);
        return File.createTempFile(prefix + bytesToHex(bytes), suffix);
    }

    public static File createTempFile(String prefix, String suffix, File directory) throws IOException {
        ensureTempDirectoryExists();
        byte[] bytes = generateRandomBytes(32);
        return File.createTempFile(prefix + bytesToHex(bytes), suffix, directory);
    }

    public static File createTempDirectory(final String prefix, final String suffix) throws IOException {
        final File tempFile = createTempFile(prefix, suffix);
        if (tempFile.exists() && !tempFile.delete()) {
            throw new SafeIoException("Unable to delete file to create directory.");
        }
        if (!tempFile.mkdir()) {
            throw new SafeIoException("Unable to create directory.");
        }
        return tempFile;
    }

    /**
     * Ensures that the file exists
     */
    public static void ensureFileExists(File file) throws IOException {
        if (!file.exists()) {
            file.createNewFile();
        }
    }

    /**
     * Ensures that the tmp directory in the java.io.tmpdir system property exists
     */
    public static void ensureTempDirectoryExists() throws IOException {
        String tmpDirectoryPath = System.getProperty("java.io.tmpdir");
        ensureDirectoryExists(tmpDirectoryPath);
    }

    /**
     * Ensures that the directory specified by the path exists
     */
    public static void ensureDirectoryExists(String path) throws IOException {
        File tmpDirectory = new File(path);
        if (!mkdirsWithRetry(tmpDirectory)) {
            throw new IOException("Failed to create directory " + tmpDirectory.getAbsolutePath());
        }
    }

    /**
     * A thread-safer mkdirs. mkdirs() will fail if another thread creates one of the directories it
     * means to create before it does. http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4742723
     *
     * This attempts the mkdirs() multiple times, so that if another thread does create one of the
     * directories, the next call to mkdirs() will take that into account.
     *
     * Note: this returns true iff the file f ends up being a directory, contrary to mkdirs(), which
     * returns false if it existed before the call.
     */
    public static boolean mkdirsWithRetry(final File f) {
        if (f.exists()) {
            return f.isDirectory();
        }

        for (int i = 0; i < 10 && !f.isDirectory(); ++i) {
            f.mkdirs();
        }

        return f.isDirectory();
    }

    private static String bytesToHex(byte... bytes) {
        return BaseEncoding.base16().lowerCase().encode(bytes);
    }
}
