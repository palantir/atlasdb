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
package com.palantir.atlasdb.keyvalue.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RangeRequests;
import org.junit.Test;

public class RangeRequestTest {

    @Test
    public void testPrefix() {
        byte[] end = RangeRequest.builder().prefixRange(new byte[] {-1}).build().getEndExclusive();
        assertThat(end).hasSize(0);
        end = RangeRequest.builder().prefixRange(new byte[] {-2}).build().getEndExclusive();
        assertThat(end).hasSize(1);
        assertThat(end[0]).isEqualTo((byte) -1);

        end = RangeRequest.builder().prefixRange(new byte[] {0, -1}).build().getEndExclusive();
        assertThat(end).hasSize(1);
        assertThat(end[0]).isEqualTo((byte) 1);

        end = RangeRequest.builder().prefixRange(new byte[] {0, -1, 0}).build().getEndExclusive();
        assertThat(end).hasSize(3);
        assertThat(end[2]).isEqualTo((byte) 1);
    }

    @Test
    public void testEmpty() {
        RangeRequest request = RangeRequest.builder()
                .endRowExclusive(RangeRequests.getFirstRowName())
                .build();
        assertThat(request.isEmptyRange()).isTrue();
        request = RangeRequest.reverseBuilder()
                .endRowExclusive(RangeRequests.getLastRowName())
                .build();
        assertThat(request.isEmptyRange()).isTrue();
    }
}
