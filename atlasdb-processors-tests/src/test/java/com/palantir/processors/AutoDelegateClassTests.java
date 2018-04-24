/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.junit.Test;

public class AutoDelegateClassTests {
    private static final Method[] TEST_CLASS_METHODS = TestClass.class.getDeclaredMethods();
    public static final Constructor<?>[] TEST_CLASS_CONSTRUCTORS = TestClass.class.getConstructors();

    @Test
    public void staticMethodsAreNotGenerated() {
        Set<String> staticMethods = extractTestClassMethods(Modifier::isStatic);
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestClass.class);

        assertThat(generatedMethods, not(hasItems(staticMethods.toArray(new String[0]))));
    }

    @Test
    public void publicInstanceMethodsAreGenerated() {
        Set<String> publicMethods = extractTestClassMethods(modifiers ->
                !Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers));
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestClass.class);

        assertThat(generatedMethods, hasItems(publicMethods.toArray(new String[0])));
    }

    @Test
    public void privateAndProtectedInstanceMethodsAreNotGenerated() {
        Set<String> privateOrProtectedMethods = extractTestClassMethods(modifiers -> !Modifier.isStatic(modifiers)
                && (Modifier.isProtected(modifiers) || Modifier.isPrivate(modifiers)));
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestClass.class);

        assertThat(generatedMethods, not(hasItems(privateOrProtectedMethods.toArray(new String[0]))));
    }

    @Test
    public void publicAndProtectedConstructorsAreGenerated() {
        Set<String> publicOrProtectedConstructors = extractTestClassConstructors(modifiers ->
                Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers));
        Set<String> generatedConstructors = TestingUtils.extractConstructors(AutoDelegate_TestClass.class);

        assertThat(generatedConstructors, is(publicOrProtectedConstructors));
    }

    @Test
    public void inheritedMethodsAreGenerated() {
        Set<String> parentMethods = extractTestClassMethods(modifiers ->
                !Modifier.isStatic(modifiers) && Modifier.isPublic(modifiers));
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_ChildClass.class);

        assertThat(generatedMethods, hasItems(parentMethods.toArray(new String[0])));
    }

    private Set<String> extractTestClassMethods(Predicate<Integer> filter) {
        return Arrays.stream(TEST_CLASS_METHODS)
                .filter(method -> filter.test(method.getModifiers()))
                .map(TestingUtils::methodToString)
                .collect(Collectors.toSet());
    }

    private Set<String> extractTestClassConstructors(Predicate<Integer> filter) {
        return Arrays.stream(TEST_CLASS_CONSTRUCTORS)
                .filter(constructor -> filter.test(constructor.getModifiers()))
                .map(TestingUtils::constructorToString)
                .collect(Collectors.toSet());
    }

}
