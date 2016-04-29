package com.palantir.atlasdb

import org.gradle.api.Plugin
import org.gradle.api.Project

class AtlasPlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.extensions.create("atlasdb", AtlasPluginExtension)
    }
}
