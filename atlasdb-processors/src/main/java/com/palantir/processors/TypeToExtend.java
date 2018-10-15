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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

import com.google.common.collect.Sets;

final class TypeToExtend {
    private TypeElement typeToExtend;
    private PackageElement typePackage;
    private Set<ExecutableElement> methods;
    private Set<ExecutableElement> constructors;

    TypeToExtend(PackageElement typePackage,
            TypeElement typeToExtend,
            TypeElement... supertypes) {

        this.typeToExtend = typeToExtend;
        this.typePackage = typePackage;

        List<ExecutableElement> allMethods = extractMethods(typeToExtend);
        for (TypeElement supertype : supertypes) {
            allMethods.addAll(extractMethods(supertype));
        }

        Map<String, ExecutableElement> methodSignatureToMethod = allMethods
                .stream()
                .collect(Collectors.toMap(ExecutableElement::toString, Function.identity(),
                        // In the case of methods with same signature, just pick any of them,
                        // since they're both the same.
                        (methodSignature1, methodSignature2) -> methodSignature1));

        methods = Sets.newHashSet(methodSignatureToMethod.values());
        constructors = extractConstructors(typeToExtend);
    }

    private List<ExecutableElement> extractMethods(TypeElement typeToExtractMethodsFrom) {
        Predicate<Element> filterForType;
        if (typeToExtractMethodsFrom.getKind() == ElementKind.INTERFACE) {
            filterForType = this::interfaceMethodFilter;
        } else {
            filterForType = this::classMethodFilter;
        }

        return typeToExtractMethodsFrom.getEnclosedElements()
                .stream()
                .filter(filterForType)
                .map(element -> (ExecutableElement) element)
                .collect(Collectors.toList());
    }

    private Boolean interfaceMethodFilter(Element element) {
        return element.getKind() == ElementKind.METHOD
                && element.getModifiers().contains(Modifier.PUBLIC);
    }

    private Boolean classMethodFilter(Element element) {
        return element.getKind() == ElementKind.METHOD
                && element.getModifiers().contains(Modifier.PUBLIC)
                && !element.getModifiers().contains(Modifier.FINAL)
                && !element.getModifiers().contains(Modifier.NATIVE)
                && !element.getModifiers().contains(Modifier.STATIC)
                && !element.getSimpleName().contentEquals("equals")
                && !element.getSimpleName().contentEquals("toString")
                && !element.getSimpleName().contentEquals("hashCode");
    }

    private Set<ExecutableElement> extractConstructors(TypeElement typeToExtractConstructorsFrom) {
        return typeToExtractConstructorsFrom.getEnclosedElements()
                .stream()
                .filter(this::constructorFilter)
                .map(element -> (ExecutableElement) element)
                .collect(Collectors.toSet());
    }

    private Boolean constructorFilter(Element element) {
        return element.getKind() == ElementKind.CONSTRUCTOR
                && element.getSimpleName().contentEquals("<init>")
                && !element.getModifiers().contains(Modifier.PRIVATE);
    }

    Boolean isPublic() {
        return typeToExtend.getModifiers().contains(Modifier.PUBLIC);
    }

    Boolean isInterface() {
        return typeToExtend.getKind() == ElementKind.INTERFACE;
    }

    String getCanonicalName() {
        return typeToExtend.getQualifiedName().toString();
    }

    String getSimpleName() {
        return typeToExtend.getSimpleName().toString();
    }

    String getPackageName() {
        return typePackage.getQualifiedName().toString();
    }

    TypeMirror getType() {
        return typeToExtend.asType();
    }

    Set<ExecutableElement> getMethods() {
        return methods;
    }

    Set<ExecutableElement> getConstructors() {
        return constructors;
    }
}
