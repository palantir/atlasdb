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

import java.lang.annotation.ElementType;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.MirroredTypesException;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

import com.squareup.javapoet.ParameterSpec;

final class ProcessorUtils {
    private ProcessorUtils() {}

    static List<TypeElement> extractExceptionsFromAnnotation(Elements elementUtils, Types typeUtils,
            AutoDelegate annotation) {
        try {
            // Throws a MirroredTypeException if the exception classes are not compiled.
            return Arrays.stream(annotation.exceptions())
                    .filter(klass -> !klass.equals(void.class))
                    .map(klass -> {
                        TypeMirror mirror = (TypeMirror) elementUtils.getTypeElement(klass.getCanonicalName());
                        return extractType(typeUtils, mirror);
                    })
                    .collect(Collectors.toList());
        } catch (MirroredTypesException mte) {
            return mte.getTypeMirrors().stream()
                    .filter(mirror -> !mirror.getKind().equals(TypeKind.VOID))
                    .map(mirror -> extractType(typeUtils, mirror))
                    .collect(Collectors.toList());
        }
    }

    static TypeElement extractTypeFromAnnotation(Elements elementUtils, AutoDelegate annotation) {
        try {
            // Throws a MirroredTypeException if the type is not compiled.
            Class typeClass = annotation.typeToExtend();
            return elementUtils.getTypeElement(typeClass.getCanonicalName());
        } catch (MirroredTypeException mte) {
            DeclaredType typeMirror = (DeclaredType) mte.getTypeMirror();
            return (TypeElement) typeMirror.asElement();
        }
    }

    static TypeElement extractType(Types typeUtils, TypeMirror typeToExtract) {
        try {
            // Throws a MirroredTypeException if the type is not compiled.
            return (TypeElement) typeUtils.asElement(typeToExtract);
        } catch (MirroredTypeException mte) {
            DeclaredType typeMirror = (DeclaredType) mte.getTypeMirror();
            return (TypeElement) typeMirror.asElement();
        }
    }

    static List<ParameterSpec> extractParameters(ExecutableElement constructor) {
        return constructor.getParameters()
                .stream()
                .map(ParameterSpec::get)
                .collect(Collectors.toList());
    }
}
