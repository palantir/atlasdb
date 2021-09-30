/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.Test;

public final class CacheValueTests {

    @Test
    public void differentClassesAreNotEqual() {
        byte[] data = {1, 2, 3};
        Optional<byte[]> value1 = Optional.of(data);
        CacheValue value2 = CacheValue.of(data);
        assertThat(value2).isNotEqualTo(value1);
        assertThat(value2).isNotEqualTo(data);
    }

    @Test
    public void differentByteInstancesAreEqual() {
        byte[] data1 = {1, 2, 3};
        byte[] data2 = {1, 2, 3};
        assertThat(data1 == data2).isFalse();
        assertThat(CacheValue.of(data1)).isEqualTo(CacheValue.of(data2));
    }

    @Test
    public void emptyValuesAreEqual() {
        assertThat(CacheValue.empty()).isEqualTo(CacheValue.empty());
    }

    @Test
    public void emptyValueNotEqualToPresentValue() {
        CacheValue presentValue = CacheValue.of(new byte[] {1, 2, 3});
        CacheValue emptyValue = CacheValue.empty();
        assertThat(presentValue).isNotEqualTo(emptyValue);
        assertThat(emptyValue).isNotEqualTo(presentValue);
    }
}
