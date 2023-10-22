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
package com.palantir.util.crypto;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.base.Charsets;
import com.google.common.io.BaseEncoding;
import java.security.MessageDigest;
import org.junit.jupiter.api.Test;

public class Sha256HashTest {

    @Test
    public void testSha256() throws Exception {
        MessageDigest digest = Sha256Hash.getMessageDigest();
        assertThat(digest.getAlgorithm()).isEqualTo("SHA-256");
        assertThat(digest).isNotSameAs(Sha256Hash.getMessageDigest());
        byte[] result = digest.digest("Hello World".getBytes(Charsets.UTF_8));
        assertThat(BaseEncoding.base16().encode(result))
                .isEqualTo("A591A6D40BF420404A011733CFB7B190D62C65BF0BCDA32B57B277D9AD9F146E");
        assertThat(result).hasSize(32);
    }
}
