/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.encoding;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class EncodingUtilsTest {
    private static final long TEST_TS = 123;

    @Test
    public void transformTimestampInvolutes() {
        assertThat(EncodingUtils.transformTimestamp(EncodingUtils.transformTimestamp(TEST_TS))).isEqualTo(TEST_TS);
    }

    @Test
    public void transformTimestampInvertsBits() {
        long expected_ts = -124;
        assertThat(EncodingUtils.transformTimestamp(TEST_TS)).isEqualTo(expected_ts);
    }

    @Test
    public void hexToStringDecodesJsonBlobCorrectly() {
        String jsonBlob = "0x7b227461626c65526566223a7b226e616d657370616365223a7b226e616d65223a22666f6f227d2c227461626c656e616d65223a22626172227d2c227374617274526f77223a2241514944222c227374617274436f6c756d6e223a22645735316332566b222c227374616c6556616c75657344656c65746564223a31302c2263656c6c547350616972734578616d696e6564223a3230302c226d696e696d756d537765707454696d657374616d70223a31323334357d";

        assertThat(EncodingUtils.hexToString(jsonBlob)).contains("gsdf");
    }
}
