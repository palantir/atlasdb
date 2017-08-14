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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

import com.google.common.collect.Sets;

final class InterfaceToExtend {
    private TypeElement interfaceToExtend;
    private PackageElement interfacePackage;
    private Set<ExecutableElement> executableElements;

    InterfaceToExtend(PackageElement interfacePackage,
            TypeElement interfaceToExtend,
            TypeElement... superInterfaces) {

        this.interfaceToExtend = interfaceToExtend;
        this.interfacePackage = interfacePackage;

        List<ExecutableElement> allMethods = extractMethods(interfaceToExtend);
        for (TypeElement superInterface : superInterfaces) {
            allMethods.addAll(extractMethods(superInterface));
        }

        Map<String, ExecutableElement> unifiedMethods = allMethods
                .stream()
                .collect(Collectors.toMap(ExecutableElement::toString, Function.identity(), (name1, name2) -> name1));

        executableElements = Sets.newHashSet(unifiedMethods.values());
    }

    private List<ExecutableElement> extractMethods(TypeElement interfaceToExtractMethodsFrom) {
        return interfaceToExtractMethodsFrom.getEnclosedElements()
                .stream()
                .filter(element -> element.getKind() == ElementKind.METHOD)
                .map(element -> (ExecutableElement) element)
                .collect(Collectors.toList());
    }

    Set<Modifier> getModifiers() {
        return interfaceToExtend.getModifiers();
    }

    String getCanonicalName() {
        return interfaceToExtend.getQualifiedName().toString();
    }

    String getSimpleName() {
        return interfaceToExtend.getSimpleName().toString();
    }

    String getPackageName() {
        return interfacePackage.getSimpleName().toString();
    }

    TypeMirror getType() {
        return interfaceToExtend.asType();
    }

    Set<ExecutableElement> getMethods() {
        return executableElements;
    }
}
