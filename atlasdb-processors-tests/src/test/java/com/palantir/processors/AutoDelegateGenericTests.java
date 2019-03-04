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

package com.palantir.processors;


import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.assertj.core.api.Condition;
import org.junit.Test;

import com.google.common.collect.Sets;

public class AutoDelegateGenericTests {
    @Test
    public void generatedInterfaceHasInterfaceMethods() {
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_GenericsTester.class);
        Set<String> originalMethods = TestingUtils.extractNonStaticMethods(GenericsTester.class);

        assertThat(generatedMethods).containsAll(originalMethods);
    }

    @Test
    public void generatedInterfaceHasDelegateMethod() {
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_GenericsTester.class);
        Set<String> originalMethods = TestingUtils.extractNonStaticMethods(GenericsTester.class);

        assertThat(Sets.difference(generatedMethods, originalMethods)).haveAtLeastOne(
                new Condition<>(string -> string.contains("delegate"), "contains the string 'delegate'"));
    }
}
