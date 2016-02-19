/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.common.concurrent;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.util.file.TempFileUtils;

public class FileLockBasedLock implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(FileLockBasedLock.class);

    private static final String LOCK_FILE_PREFIX = ".pt_kv_lock";
    private final RandomAccessFile lockFile;
    private final FileLock flock;
    private volatile boolean closed = false;

    private FileLockBasedLock(RandomAccessFile lockFile, FileLock flock) {
        this.lockFile = lockFile;
        this.flock = flock;
    }

    /**
     * @return token to unlock the lock
     * @throws IOException if locking fails
     */
    public static FileLockBasedLock lockDirectory(File directoryToLock) throws IOException {
        TempFileUtils.mkdirsWithRetry(directoryToLock);
        Preconditions.checkArgument(directoryToLock.exists() && directoryToLock.isDirectory(), "DB file must be a directory: " + directoryToLock);
        final RandomAccessFile randomAccessFile =
            new RandomAccessFile(
                /* NB: We don't want to  write files into the directory directly. Exta files in the directory may throw
                       off whoever is using it. Rather than trying to trick it into accepting our lock file name as
                       valid, we write the file as a sibling to the database directory, constructing its name to
                       include the name of the database directory as a suffix.

                   NB: The documentation for File#deleteOnExit() advises against using it in concert with file locking,
                       so we'll allow this file to remain in place after the program exits.
                       See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4676183
                 */
                new File(directoryToLock.getParentFile(),
                         String.format("%s_%s", LOCK_FILE_PREFIX, directoryToLock.getName())),
                "rws");
        final FileChannel channel = randomAccessFile.getChannel();
        boolean success = false;
        try {
            final FileLock lock = channel.tryLock();
            if (lock == null) {
                throw new IOException("Cannot lock. Someone already has this database open: " + directoryToLock);
            }
            success = true;
            return new FileLockBasedLock(randomAccessFile, lock);
        } catch (OverlappingFileLockException e) {
            throw new IOException("Cannot lock. This jvm already has this database open: " + directoryToLock);
        } finally {
            if (!success) {
                randomAccessFile.close();
            }
        }
    }

    public void unlock() {
        close();
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            try {
                flock.release();
                lockFile.close();
            } catch (IOException e) {
                log.warn("failed to close");
            }
        }
    }

}
