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

import com.squareup.javapoet.ParameterSpec;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.MirroredTypeException;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Types;

final class ProcessorUtils {
    private ProcessorUtils() {}

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
        return constructor.getParameters().stream().map(ParameterSpec::get).collect(Collectors.toList());
    }
}
