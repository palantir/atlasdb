/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.errorprone;

import com.google.common.base.Predicates;
import com.google.errorprone.CompilationTestHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultClassMustBeFinalTest {
    private CompilationTestHelper compilationHelper;

    @BeforeEach
    void before() {
        compilationHelper = CompilationTestHelper.newInstance(DefaultClassMustBeFinal.class, getClass());
    }

    @Test
    void testPass() {
        compilationHelper
                .addSourceFile("DefaultClassIsFinal.java")
                .expectNoDiagnostics()
                .doTest();
    }

    @Test
    void testFail() {
        compilationHelper
                .addSourceFile("DefaultClassIsNotFinal.java")
                .expectErrorMessage(
                        "DefaultClassMustBeFinal",
                        Predicates.containsPattern("Default implementation classes must be marked as final"))
                .doTest();
    }
}
