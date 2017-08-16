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

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

public class AutoDelegateClassTests {
    private static final Method[] TEST_CLASS_METHODS = TestClass.class.getDeclaredMethods();

    @Test
    public void staticMethodsAreNotGenerated() {
        Set<String> staticMethods = Arrays.stream(TEST_CLASS_METHODS)
                .filter(method -> Modifier.isStatic(method.getModifiers()))
                .map(TestingUtils::methodToString)
                .collect(Collectors.toSet());
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestClass.class);

        assertThat(generatedMethods, not(hasItems(staticMethods.toArray(new String[0]))));
    }

    @Test
    public void publicInstanceMethodsAreGenerated() {
        Set<String> publicMethods = Arrays.stream(TEST_CLASS_METHODS)
                .filter(method -> !Modifier.isStatic(method.getModifiers())
                                && Modifier.isPublic(method.getModifiers()))
                .map(TestingUtils::methodToString)
                .collect(Collectors.toSet());
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestClass.class);

        assertThat(generatedMethods, hasItems(publicMethods.toArray(new String[0])));
    }

    @Test
    public void privateAndProtectedInstanceMethodsAreNotGenerated() {
        Set<String> privateOrProtectedMethods = Arrays.stream(TEST_CLASS_METHODS)
                .filter(method -> !Modifier.isStatic(method.getModifiers())
                        && (Modifier.isProtected(method.getModifiers()) || Modifier.isPrivate(method.getModifiers())))
                .map(TestingUtils::methodToString)
                .collect(Collectors.toSet());
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestClass.class);

        assertThat(generatedMethods, not(hasItems(privateOrProtectedMethods.toArray(new String[0]))));
    }

    @Test
    public void publicAndProtectedConstructorsAreGenerated() {
        Set<String> publicOrProtectedConstructors = Arrays.stream(TestClass.class.getConstructors())
                .filter(constructor -> Modifier.isPublic(constructor.getModifiers())
                        || Modifier.isProtected(constructor.getModifiers()))
                .map(TestingUtils::constructorToString)
                .collect(Collectors.toSet());
        Set<String> generatedConstructors = TestingUtils.extractConstructors(AutoDelegate_TestClass.class);

        assertThat(generatedConstructors, is(publicOrProtectedConstructors));
    }

    @Test
    public void inheritedMethodsAreGenerated() {
        Set<String> parentMethods = Arrays.stream(TEST_CLASS_METHODS)
                .filter(method -> !Modifier.isStatic(method.getModifiers())
                        && Modifier.isPublic(method.getModifiers()))
                .map(TestingUtils::methodToString)
                .collect(Collectors.toSet());
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_ChildClass.class);

        assertThat(generatedMethods, hasItems(parentMethods.toArray(new String[0])));
    }
}
