/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.config;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.palantir.config.crypto.KeyPair;

public class AtlasDbConfigsTest {
    private static String previousKeyPathProperty;

    @BeforeClass
    public static void setUpClass() throws URISyntaxException {
        previousKeyPathProperty = System.getProperty(KeyPair.KEY_PATH_PROPERTY);
        System.setProperty(
                KeyPair.KEY_PATH_PROPERTY,
                Paths.get(AtlasDbConfigsTest.class.getResource("/test.key").toURI()).toString());
    }

    @AfterClass
    public static void tearDownClass() {
        if (previousKeyPathProperty != null) {
            System.setProperty(KeyPair.KEY_PATH_PROPERTY, previousKeyPathProperty);
        }
    }

    @Test
    public void canDecryptValues() throws IOException {
        AtlasDbConfigs.load(new File(AtlasDbConfigsTest.class.getResource("/encrypted-config.yml").getPath()),
                AtlasDbConfig.class);
    }
}
