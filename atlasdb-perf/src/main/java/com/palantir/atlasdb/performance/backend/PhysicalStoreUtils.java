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
 *
 */

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
