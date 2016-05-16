package com.palantir.atlasdb

import com.palantir.gradle.javadist.JavaDistributionPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin

class AtlasPlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.plugins.apply(JavaPlugin)
        project.plugins.apply(JavaDistributionPlugin)
        project.extensions.create("atlasdb", AtlasPluginExtension)
    }
}