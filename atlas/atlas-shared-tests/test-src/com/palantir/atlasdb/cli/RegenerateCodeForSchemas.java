// Copyright 2015 Palantir Technologies
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.palantir.atlasdb.cli;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.indexing.IndexTestSchema;
import com.palantir.atlasdb.schema.stream.StreamTestSchema;

/**
 * Regenerate all atlas schemas, useful when you touch TableRenderer.
 * You'll still need to manually fix up DeprecatedReadStateService's stuff and DeprecatedPacmanNote01 because of past mistakes.
 *
 * @author clockfort
 *
 */
public class RegenerateCodeForSchemas {

    public static void main(String[] args) {
        List<AtlasSchema> schemas = Lists.newArrayList();
        schemas.add(IndexTestSchema.INSTANCE);
        schemas.add(StreamTestSchema.INSTANCE);

        for (AtlasSchema schema : schemas) {
            try {
                System.out.println("Attempting to generate source code for schema: " + schema.getClass().getCanonicalName()); // (authorized)

                URL location = schema.getClass().getProtectionDomain().getCodeSource().getLocation();
                System.out.println("Generating source directory related to classes in " + location); // (authorized)
                if (location.toString().endsWith("jar")) {
                    System.err.println("You need to set your classpaths to not come from jars / ivy / whatever."); // (authorized)
                    System.exit(1);
                }

                String sourceDirLocation = location.getFile() + "../src";
                if (schema.getClass().getCanonicalName().contains("Test")) {
                    sourceDirLocation = location.getFile() + "../test-src";
                }

                if (new File(sourceDirLocation).exists()) {
                    System.out.println("Placing source in: " + sourceDirLocation +"\n"); // (authorized)
                    schema.getLatestSchema().renderTables(new File(sourceDirLocation));
                } else {
                    System.out.println("Didn't find anything at " + sourceDirLocation + ", trying another directory..."); // (authorized)
                    sourceDirLocation = location.getFile() + "../atlasdb-src"; // wtf pacman / aggro, wtf
                    if (new File(sourceDirLocation).exists()) {
                        System.out.println("Placing source in: " + sourceDirLocation +"\n"); // (authorized)
                        schema.getLatestSchema().renderTables(new File(sourceDirLocation));
                    } else {
                        System.err.println("Could not find a matching source code directory for this project."); // (authorized)
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
