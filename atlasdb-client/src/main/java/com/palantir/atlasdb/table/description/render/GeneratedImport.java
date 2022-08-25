/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.table.description.render;

import com.squareup.javapoet.ClassName;
import javax.lang.model.SourceVersion;

/**
 * From autovalue's GeneratedImport class
 */
final class GeneratedImport {
    /**
     * Returns the qualified name of the {@code @Generated} annotation available during a compilation
     * task.
     */
    static String generatedAnnotationType() {
        return SourceVersion.latestSupported().compareTo(SourceVersion.RELEASE_8) > 0
                ? "javax.annotation.processing.Generated"
                : "javax.annotation.Generated";
    }

    static ClassName generatedAnnotationClassName() {
        return SourceVersion.latestSupported().compareTo(SourceVersion.RELEASE_8) > 0
                ? ClassName.get("javax.annotation.processing", "Generated")
                : ClassName.get("javax.annotation", "Generated");
    }
}
