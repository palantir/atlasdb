package com.palantir.atlasdb

import com.palantir.gradle.javadist.AtlasCreateStartScriptsTask
import org.gradle.api.InvalidUserDataException
import org.gradle.api.Plugin
import org.gradle.api.Project

class AtlasPlugin implements Plugin<Project> {
    @Override
    void apply(Project project) {
        project.plugins.apply 'java'
        AtlasPluginExtension ext = project.extensions.create('atlasdb', AtlasPluginExtension)

        AtlasCreateStartScriptsTask createScript = project.tasks.create('createAtlasScripts', AtlasCreateStartScriptsTask)

        project.repositories {
            mavenCentral()
            maven {
                url 'http://dl.bintray.com/palantir/releases/'
            }
        }

        project.afterEvaluate {
            // if the java-distribution plugin is applied make the DistTarTask build the script
            project.tasks.findByName('distTar')?.dependsOn createScript

            if (!ext.atlasVersion?.trim()) {
                throw new InvalidUserDataException("You must define a atlasVersion in your build.gradle atlas block!")
            }

            project.dependencies {
                runtime "com.palantir.atlasdb:atlasdb-cli:$ext.atlasVersion"
            }
        }
    }
}