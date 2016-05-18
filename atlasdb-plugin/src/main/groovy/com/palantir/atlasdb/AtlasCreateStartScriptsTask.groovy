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