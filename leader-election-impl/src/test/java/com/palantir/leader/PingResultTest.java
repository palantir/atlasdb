/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.leader;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.sls.versions.OrderableSlsVersion;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class PingResultTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void pingResultIsJavaSerializable() throws IOException, ClassNotFoundException {
        OrderableSlsVersion timeLockVersion = OrderableSlsVersion.valueOf("0.27.0");
        PingResult pr = PingResult.builder()
                .timeLockVersion(timeLockVersion)
                .isLeader(true)
                .build();

        File file = tempFolder.newFile();
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file))) {
            out.writeObject(pr);
        }

        try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file))) {
            PingResult deserializedPingResult = (PingResult) in.readObject();
            assertThat(deserializedPingResult.isLeader()).isTrue();
            assertThat(deserializedPingResult.timeLockVersion()).isEqualTo(Optional.of(timeLockVersion));
        }
    }
}
