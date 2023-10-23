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

package com.palantir.timelock.store;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

public class BlobStoreUseCaseTest {
    @Test
    public void useCasesDoNotClashOnShortNames() {
        Set<String> rowNames = Arrays.stream(BlobStoreUseCase.values())
                .map(BlobStoreUseCase::getShortName)
                .collect(Collectors.toSet());
        assertThat(rowNames).as("use cases should have distinct short names").hasSameSizeAs(BlobStoreUseCase.values());
    }
}
