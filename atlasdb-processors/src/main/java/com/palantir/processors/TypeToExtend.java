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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.TypeParameterElement;
import javax.lang.model.type.TypeMirror;

final class TypeToExtend {
    private final TypeElement typeToExtend;
    private final PackageElement typePackage;
    private final Set<ExecutableElement> methods;

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
                        (methodSignature1, _methodSignature2) -> methodSignature1));

        methods = new HashSet<>(methodSignatureToMethod.values());
    }

    private static List<ExecutableElement> extractMethods(TypeElement typeToExtractMethodsFrom) {
        return typeToExtractMethodsFrom.getEnclosedElements()
                .stream()
                .filter(TypeToExtend::interfaceMethodFilter)
                .map(element -> (ExecutableElement) element)
                .collect(Collectors.toList());
    }

    private static boolean interfaceMethodFilter(Element element) {
        return element.getKind() == ElementKind.METHOD
                && element.getModifiers().contains(Modifier.PUBLIC)
                && !element.getModifiers().contains(Modifier.STATIC)
                && element.getAnnotation(DoNotDelegate.class) == null;
    }

    boolean isPublic() {
        return typeToExtend.getModifiers().contains(Modifier.PUBLIC);
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

    List<? extends TypeParameterElement> getTypeParameterElements() {
        return typeToExtend.getTypeParameters();
    }
}
