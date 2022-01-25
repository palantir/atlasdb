/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.common.concurrent;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.Test;

public final class ThreadNamesTest {

    @Test
    public void testRenameCurrentThread() {
        Thread current = Thread.currentThread();
        String originalName = current.getName();
        try {
            String newName = UUID.randomUUID().toString();
            ThreadNames.setThreadName(current, newName);
            assertThat(current.getName()).isEqualTo(newName);
        } finally {
            current.setName(originalName);
        }
        assertThat(current.getName()).isEqualTo(originalName);
    }

    @Test
    public void testRenameAppearsInThreadDump() throws Exception {
        Thread current = Thread.currentThread();
        String originalName = current.getName();
        try {
            String newName = UUID.randomUUID().toString();
            ThreadNames.setThreadName(current, newName);
            assertThat(current.getName()).isEqualTo(newName);

            // Use the current JVM, not 'jstack' from the PATH
            String jstackPath = new File(System.getProperty("java.home"), "bin/jstack").getAbsolutePath();

            Process process = Runtime.getRuntime().exec(new String[] {
                jstackPath, Long.toString(ProcessHandle.current().pid())
            });
            String jstackOutput;
            try (InputStream data = process.getInputStream()) {
                jstackOutput = new String(ByteStreams.toByteArray(data), StandardCharsets.UTF_8);
            }
            int exitCode = process.waitFor();
            assertThat(exitCode).as("Jstack exited non-zero").isZero();
            assertThat(jstackOutput).contains('"' + newName + "\" ");
        } finally {
            current.setName(originalName);
        }
        assertThat(current.getName()).isEqualTo(originalName);
    }
}
