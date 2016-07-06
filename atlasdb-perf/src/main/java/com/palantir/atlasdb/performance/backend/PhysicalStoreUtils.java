package com.palantir.atlasdb.performance.backend;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * Static utilities class for methods pertaining to physical stores.
 *
 * @author mwakerman
 */
public class PhysicalStoreUtils {

    public static File writeResourceToTempFile(Class clazz, String resourcePath) throws IOException {
        URL resource = clazz.getResource(resourcePath);

        File file = File.createTempFile(
                FilenameUtils.getBaseName(resource.getFile()),
                FilenameUtils.getExtension(resource.getFile()));

        IOUtils.copy(resource.openStream(), FileUtils.openOutputStream(file));
        file.deleteOnExit();
        return file;
    }
}
