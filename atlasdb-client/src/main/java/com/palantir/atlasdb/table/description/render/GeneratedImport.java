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
