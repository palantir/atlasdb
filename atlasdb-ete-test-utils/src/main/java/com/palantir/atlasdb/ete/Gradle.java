/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.ete;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;
import org.junit.rules.ExternalResource;

public final class Gradle extends ExternalResource {

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
            System.out.println("It looks like you are not running in gradle,"
                    + " performing the required gradle command: " + command); // (authorized)
            gradle();
        } else {
            System.out.println("You are running in gradle," + " allowing gradle task dependencies to make sure "
                    + command + " is run"); // (authorized)
        }
    }

    private void gradle() {
        try {
            Path pathToGradlew = Paths.get("..", "gradlew");

            Process process = new ProcessBuilder(pathToGradlew.toFile().getAbsolutePath(), command)
                    .redirectErrorStream(true)
                    .start();
            try (InputStream processInputStream = process.getInputStream()) {
                IOUtils.copy(processInputStream, System.out);
            }
            assertThat(process.waitFor()).isEqualTo(0);
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
