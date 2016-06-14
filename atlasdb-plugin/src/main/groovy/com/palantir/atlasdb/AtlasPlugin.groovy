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