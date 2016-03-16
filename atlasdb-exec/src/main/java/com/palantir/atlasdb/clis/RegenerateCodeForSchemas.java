package com.palantir.atlasdb.clis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.SweepSchema;

/**
 * Regenerate all atlas schemas, useful when you touch TableRenderer.
 *
 */
public class RegenerateCodeForSchemas {

    public static void main(String[] args) {
        List<AtlasSchema> schemas = Lists.newArrayList(SweepSchema.INSTANCE);

        for (AtlasSchema schema : schemas) {
            try {
                System.out.println("Attempting to generate source code for schema: " + schema.getClass().getCanonicalName());

                Path location = Paths.get(schema.getClass().getProtectionDomain().getCodeSource().getLocation().getPath());
                System.out.println("Generating source directory related to classes in " + location);
                if (location.toString().endsWith("jar")) {
                    System.err.println("You need to set your classpaths to not come from jars / ivy / whatever.");
                    System.exit(1);
                }

                Path sourceDirLocation = location.resolve("../src");
                if (schema.getClass().getCanonicalName().contains("Test")) {
                    sourceDirLocation = location.resolve("../test-src");
                }

                if (!Files.exists(sourceDirLocation)) {
                    System.out.println("Didn't find anything at " + sourceDirLocation + ", trying another directory...");
                    sourceDirLocation = resolveIdeaSourceLocation(sourceDirLocation);
                    if (!Files.exists(sourceDirLocation)) {
                        System.err.println("Could not find a matching source code directory for this project.");
                        continue;
                    }
                }
                System.out.println("Placing source in: " + sourceDirLocation + "\n");
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
