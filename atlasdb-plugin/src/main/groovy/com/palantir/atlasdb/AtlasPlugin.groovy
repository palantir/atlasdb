package com.palantir.atlasdb

import com.palantir.gradle.javadist.AtlasCreateStartScriptsTask
import com.palantir.gradle.javadist.DistTarTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin

class AtlasPlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.plugins.apply(JavaPlugin)
        AtlasPluginExtension ext = project.extensions.create('atlasdb', AtlasPluginExtension)

        AtlasCreateStartScriptsTask createScript = project.tasks.create('createAtlasScripts', AtlasCreateStartScriptsTask)

        // If the java-distribution plugin is applied make the DistTarTask build the script
        project.tasks.withType(DistTarTask)[0]?.dependsOn createScript

        // TODO: figure out how to get people to bring in the dep
        // probably should force them to specify it themselves...
//        project.beforeEvaluate {
//            project.repositories {
//                mavenCentral()
//                maven {
//                    url 'http://dl.bintray.com/palantir/releases/'
//                }
//            }
//            project.dependencies {
//                compile "com.palantir.atlasdb:atlasdb-cli:0.3.34"
//            }
//        }
    }
}