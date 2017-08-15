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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

public class AutoDelegateTests {
    @Test
    public void generatedInterfaceHasSamePackageAsOriginal() {
        Package generatedInterfacePackage = AutoDelegate_TestInterface.class.getPackage();
        Package originalInterfacePackage = TestInterface.class.getPackage();

        assertThat(generatedInterfacePackage, is(originalInterfacePackage));
    }

    @Test
    public void generatedInterfaceIsInterface() {
        int generatedInterfaceModifiers = AutoDelegate_TestInterface.class.getModifiers();

        assertThat(Modifier.isInterface(generatedInterfaceModifiers), is(true));
    }

    @Test
    public void publicInterfacesGeneratePublicInterfaces() {
        int originalModifiers = TestInterface.class.getModifiers();
        int generatedInterfaceModifiers = AutoDelegate_TestInterface.class.getModifiers();

        assertThat(generatedInterfaceModifiers, is(originalModifiers));
    }

    @Test
    public void packagePrivateInterfacesGeneratePackagePrivateInterfaces() {
        int originalModifiers = PackagePrivateInterface.class.getModifiers();
        int generatedInterfaceModifiers = AutoDelegate_PackagePrivateInterface.class.getModifiers();

        assertThat(generatedInterfaceModifiers, is(originalModifiers));
    }

    @Test
    public void generatedInterfaceHasInterfaceMethods() {
        Set<String> generatedMethods = extractMethods(AutoDelegate_TestInterface.class);
        Set<String> originalMethods = extractMethods(TestInterface.class);

        assertThat(generatedMethods, hasItems(originalMethods.toArray(new String[0])));
    }

    @Test
    public void generatedInterfaceHasDelegateMethod() {
        Set<String> generatedMethods = extractMethods(AutoDelegate_TestInterface.class);
        Set<String> originalMethods = extractMethods(TestInterface.class);

        generatedMethods.removeAll(originalMethods);
        assertThat(generatedMethods.size(), is(1));
        assertThat(generatedMethods.iterator().next(), containsString("delegate"));
    }

    @Test
    public void childInterfaceHasParentAndChildMethods() {
        Set<String> generatedMethods = extractMethods(AutoDelegate_ChildTestInterface.class);
        Set<String> parentMethods = extractMethods(TestInterface.class);
        Set<String> childMethods = extractMethods(ChildTestInterface.class);

        assertThat(generatedMethods, hasItems(parentMethods.toArray(new String[0])));
        assertThat(generatedMethods, hasItems(childMethods.toArray(new String[0])));
    }

    @Test
    public void generatedInterfaceCallsMethodOnDelegate() {
        TestInterfaceImpl mockImpl = mock(TestInterfaceImpl.class);
        AutoDelegate_TestInterface instanceOfInterface = () -> mockImpl;

        instanceOfInterface.methodWithReturnType();
        verify(mockImpl, times(1)).methodWithReturnType();
    }

    private Set<String> extractMethods(Class childInterface) {
        return Arrays.stream(childInterface.getDeclaredMethods())
                .map(this::methodToString)
                .collect(Collectors.toSet());
    }

    private String methodToString(Method method) {
        return String.format("%s,%s,%s",
                method.getReturnType(),
                method.getName(),
                extractParameterTypes(method));
    }

    private String extractParameterTypes(Method method) {
        return Arrays.stream(method.getParameterTypes())
                .map(Class::getCanonicalName)
                .collect(Collectors.toList())
                .toString();
    }
}
