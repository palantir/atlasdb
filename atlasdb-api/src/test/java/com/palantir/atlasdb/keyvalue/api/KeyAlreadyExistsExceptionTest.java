/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.encoding.PtBytes;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import org.junit.Test;

public class KeyAlreadyExistsExceptionTest {
    @Test
    public void canDeserializeLegacyVersionsOfException() throws IOException, ClassNotFoundException {
        // The legacy exception was created on AtlasDB 0.116.1.
        URL exceptionSerializedFormUrl = KeyAlreadyExistsExceptionTest.class.getClassLoader()
                .getResource("serializedLegacyKeyAlreadyExistsException.dat");
        FileInputStream fileInputStream = new FileInputStream(new File(exceptionSerializedFormUrl.getPath()));
        KeyAlreadyExistsException deserialized = (KeyAlreadyExistsException)
                new ObjectInputStream(fileInputStream).readObject();

        assertThat(deserialized.getMessage()).isEqualTo("aaa");
        assertThat(deserialized.getCause()).isNull();
        assertThat(deserialized.getExistingKeys()).containsExactly(
                Cell.create(PtBytes.toBytes("row"), PtBytes.toBytes("col")));
        assertThat(deserialized.getKnownSuccessfullyCommittedKeys()).isEqualTo(ImmutableList.of());
    }
}
