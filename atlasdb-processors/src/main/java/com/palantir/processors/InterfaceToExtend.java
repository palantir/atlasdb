package com.palantir.processors;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
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
