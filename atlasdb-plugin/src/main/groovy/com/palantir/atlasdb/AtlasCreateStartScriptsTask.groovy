package com.palantir.gradle.javadist

import com.palantir.atlasdb.cli.AtlasCli
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputDirectory
import org.gradle.jvm.application.tasks.CreateStartScripts

class AtlasCreateStartScriptsTask extends CreateStartScripts {

    @Input
    @Override
    public String getMainClassName() {
        return AtlasCli.class.getCanonicalName()
    }

    @Input
    @Override
    public String getApplicationName() {
        return "atlas"
    }

    @OutputDirectory
    @Override
    public File getOutputDir() {
        return new File("${project.buildDir}/scripts")
    }

    @InputFiles
    @Override
    public FileCollection getClasspath() {
        return project.tasks.jar.outputs.files + project.configurations.runtime
    }

}