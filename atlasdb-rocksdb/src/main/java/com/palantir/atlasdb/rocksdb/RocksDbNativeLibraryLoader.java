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
