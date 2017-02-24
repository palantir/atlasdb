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
package com.palantir.atlasdb.clis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.SweepSchema;

/**
 * Regenerate all atlas schemas, useful when you touch TableRenderer.
 *
 */
public class RegenerateCodeForSchemas {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(RegenerateCodeForSchemas.class);

    public static void main(String[] args) {
        List<AtlasSchema> schemas = Lists.newArrayList(SweepSchema.INSTANCE);

        for (AtlasSchema schema : schemas) {
            try {
                log.info("Attempting to generate source code for schema: " + schema.getClass().getCanonicalName());

                Path location = Paths.get(schema.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
                log.info("Generating source directory related to classes in " + location);
                if (location.toString().endsWith("jar")) {
                    log.error("You need to set your classpaths to not come from jars / ivy / whatever.");
                    System.exit(1);
                }

                Path sourceDirLocation = location.resolve("../src/main/java");
                if (schema.getClass().getCanonicalName().contains("Test")) {
                    sourceDirLocation = location.resolve("../test-src");
                }

                if (!Files.exists(sourceDirLocation)) {
                    log.info("Didn't find anything at " + sourceDirLocation + ", trying another directory...");
                    sourceDirLocation = resolveIdeaSourceLocation(sourceDirLocation);
                    if (!Files.exists(sourceDirLocation)) {
                        log.error("Could not find a matching source code directory for this project.");
                        continue;
                    }
                }
                log.info("Placing source in: " + sourceDirLocation + "\n");
                schema.getLatestSchema().renderTables(sourceDirLocation.toFile());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Path resolveIdeaSourceLocation(Path originalPath) {
        String replacement = originalPath.toString().replaceFirst("out/production/", "");
        replacement = replacement.replaceFirst("\\..", "");
        return Paths.get(replacement);
    }
}
