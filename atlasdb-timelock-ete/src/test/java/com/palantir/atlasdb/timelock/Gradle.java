/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.junit.rules.ExternalResource;

public class Gradle extends ExternalResource {

    private final String command;

    private Gradle(String command) {
        this.command = command;
    }

    public static Gradle ensureTaskHasRun(String command) {
        return new Gradle(command);
    }

    @Override
    protected void before() throws Throwable {
        if (isRunningOutsideOfGradle()) {
            System.out.println("You are not running in gradle, performing the required gradle command: " + command);
            gradle(command);
        }
    }

    private void gradle(String command) {
        try {
            Path pathToGradlew = Paths.get("..", "gradlew");

            Process process = new ProcessBuilder(pathToGradlew.toFile().getAbsolutePath(), command)
                    .redirectErrorStream(true)
                    .start();
            IOUtils.copy(process.getInputStream(), System.out);
            assertThat(process.waitFor(), is(equalTo(0)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public boolean isRunningOutsideOfGradle() {
        return System.getProperty("RUNNING_IN_GRADLE", "false").equals("false");
    }
}
