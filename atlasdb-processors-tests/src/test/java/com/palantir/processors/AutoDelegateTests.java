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

package com.palantir.processors;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

import processors.AutoDelegate_ChildInterface;
import processors.AutoDelegate_RandomInterface;

public class AutoDelegateTests {
    @Test
    public void randomInterfaceComparison() {
        Set<String> generatedMethods = extractMethods(AutoDelegate_RandomInterface.class);
        Set<String> expectedMethods = extractMethods(ExpectedRandomInterface.class);

        assertTrue(generatedMethods.containsAll(expectedMethods));
        assertTrue(expectedMethods.containsAll(generatedMethods));
    }

    @Test
    public void childInterfaceComparison() {
        Set<String> generatedMethods = extractMethods(AutoDelegate_ChildInterface.class);
        Set<String> expectedMethods = extractMethods(ExpectedChildInterface.class);

        assertTrue(generatedMethods.containsAll(expectedMethods));
        assertTrue(expectedMethods.containsAll(generatedMethods));
    }

    private Set<String> extractMethods(Class childInterface) {
        return Arrays.stream(childInterface.getDeclaredMethods())
                .map(method -> String.format("%s,%s,%s",
                        method.getReturnType(),
                        method.getName(),
                        extractParameterTypes(method)))
                .collect(Collectors.toSet());
    }

    private String extractParameterTypes(Method method) {
        return Arrays.stream(method.getParameterTypes())
                .map(Class::getCanonicalName)
                .collect(Collectors.toList())
                .toString();
    }
}
