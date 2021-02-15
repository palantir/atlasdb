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

import static com.palantir.processors.TestingUtils.extractMethodsSatisfyingPredicate;
import static com.palantir.processors.TestingUtils.extractNonStaticMethods;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Sets;
import java.lang.reflect.Modifier;
import java.util.Set;
import org.junit.Test;

public class AutoDelegateInterfaceTests {

    @Test
    public void generatedInterfaceHasSamePackageAsOriginal() {
        Package generatedInterfacePackage = AutoDelegate_TestInterface.class.getPackage();
        Package originalInterfacePackage = TestInterface.class.getPackage();

        assertThat(generatedInterfacePackage).isEqualTo(originalInterfacePackage);
    }

    @Test
    public void generatedInterfaceIsInterface() {
        int generatedInterfaceModifiers = AutoDelegate_TestInterface.class.getModifiers();

        assertThat(Modifier.isInterface(generatedInterfaceModifiers)).isTrue();
    }

    @Test
    public void publicInterfacesGeneratePublicInterfaces() {
        int originalModifiers = TestInterface.class.getModifiers();
        int generatedInterfaceModifiers = AutoDelegate_TestInterface.class.getModifiers();

        assertThat(generatedInterfaceModifiers).isEqualTo(originalModifiers);
    }

    @Test
    public void packagePrivateInterfacesGeneratePackagePrivateInterfaces() {
        int originalModifiers = PackagePrivateInterface.class.getModifiers();
        int generatedInterfaceModifiers = AutoDelegate_PackagePrivateInterface.class.getModifiers();

        assertThat(generatedInterfaceModifiers).isEqualTo(originalModifiers);
    }

    @Test
    public void generatedInterfaceHasInterfaceMethods() {
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestInterface.class);
        Set<String> originalMethods = extractNonStaticMethods(TestInterface.class);
        Set<String> doNotDelegateMethods = doNotDelegateMethods(originalMethods);
        assertThat(generatedMethods)
                .containsAll(Sets.difference(originalMethods, doNotDelegateMethods))
                .doesNotContainAnyElementsOf(doNotDelegateMethods);
    }

    @Test
    public void generatedInterfaceHasDelegateMethod() {
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestInterface.class);
        Set<String> originalMethods = extractNonStaticMethods(TestInterface.class);

        generatedMethods.removeAll(originalMethods);
        assertThat(generatedMethods).hasSize(1).allMatch(s -> s.contains("delegate"));
    }

    @Test
    public void generatedInterfaceDoesNotHaveStaticMethods() {
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_TestInterface.class);
        Set<String> originalStaticMethods = extractMethodsSatisfyingPredicate(
                TestInterface.class, method -> Modifier.isStatic(method.getModifiers()));

        assertThat(Sets.intersection(generatedMethods, originalStaticMethods)).isEmpty();
    }

    @Test
    public void childInterfaceHasParentAndChildMethods() {
        Set<String> generatedMethods = TestingUtils.extractMethods(AutoDelegate_ChildTestInterface.class);
        Set<String> parentMethods = extractNonStaticMethods(TestInterface.class);
        Set<String> childMethods = extractNonStaticMethods(ChildTestInterface.class);

        Set<String> doNotDelegateMethods = doNotDelegateMethods(parentMethods);
        assertThat(generatedMethods)
                .containsAll(Sets.difference(parentMethods, doNotDelegateMethods))
                .containsAll(childMethods);
    }

    @Test
    public void generatedInterfaceCallsMethodOnDelegate() {
        TestInterfaceImpl mockImpl = mock(TestInterfaceImpl.class);
        AutoDelegate_TestInterface instanceOfInterface = new AnImpl(mockImpl);

        instanceOfInterface.methodWithReturnType();
        verify(mockImpl, times(1)).methodWithReturnType();
    }

    private static Set<String> doNotDelegateMethods(Set<String> originalMethods) {
        return Sets.filter(originalMethods, m -> m.contains("methodThatMustBeImplemented"));
    }

    static class AnImpl implements AutoDelegate_TestInterface {

        private final TestInterface delegate;

        AnImpl(TestInterface delegate) {
            this.delegate = delegate;
        }

        @Override
        public TestInterface delegate() {
            return delegate;
        }

        @Override
        public void methodThatMustBeImplemented() {}
    }
}
