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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.palantir.goethe.Goethe;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.FilerException;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;

@AutoService(Processor.class)
public final class AutoDelegateProcessor extends AbstractProcessor {
    // We keep track of if this processor has been registered in a processing environment, to avoid registering it
    // twice. Therefore, we keep weak references to both the keys and the values, to avoid keeping such references in
    // memory unnecessarily.
    private static final ConcurrentMap<ProcessingEnvironment, Processor> registeredProcessors = new MapMaker()
            .weakKeys()
            .weakValues()
            .concurrencyLevel(1)
            .initialCapacity(1)
            .makeMap();
    private static final String PREFIX = "AutoDelegate_";
    private static final String DELEGATE_METHOD = "delegate";

    private Types typeUtils;
    private Elements elementUtils;
    private Filer filer;
    private Messager messager;
    private AtomicBoolean abortProcessing = new AtomicBoolean(false);

    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);

        typeUtils = processingEnv.getTypeUtils();
        elementUtils = processingEnv.getElementUtils();
        filer = processingEnv.getFiler();
        messager = processingEnv.getMessager();

        if (registeredProcessors.putIfAbsent(processingEnv, this) != null) {
            messager.printMessage(
                    Diagnostic.Kind.NOTE, "AutoDelegate processor registered twice; disabling duplicate instance");
            abortProcessing.set(true);
        }
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        return ImmutableSet.of(AutoDelegate.class.getCanonicalName());
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.RELEASE_11;
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (abortProcessing.get()) {
            // Another instance of AutoDelegateProcessor is running in the current processing environment.
            return false;
        }

        Set<String> generatedTypes = new HashSet<>();
        for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(AutoDelegate.class)) {
            try {
                TypeElement typeElement = validateAnnotatedElement(annotatedElement);
                TypeToExtend typeToExtend = createTypeToExtend(typeElement);

                if (generatedTypes.contains(typeToExtend.getCanonicalName())) {
                    continue;
                }

                validateMethodsToBeAutoDelegated(typeElement, typeToExtend);

                generatedTypes.add(typeToExtend.getCanonicalName());

                generateCode(typeToExtend);
            } catch (FilerException e) {
                // Happens when same file is written twice.
                warn(annotatedElement, e.getMessage());
            } catch (ProcessingException e) {
                error(e.getElement(), e.getMessage());
            } catch (IOException | RuntimeException e) {
                error(annotatedElement, e.getMessage());
            }
        }

        return false;
    }

    private static TypeElement validateAnnotatedElement(Element annotatedElement) throws ProcessingException {
        ElementKind kind = annotatedElement.getKind();
        if (kind != ElementKind.INTERFACE) {
            throw new ProcessingException(
                    annotatedElement, "Only interfaces can be annotated with @%s", AutoDelegate.class.getSimpleName());
        }

        return (TypeElement) annotatedElement;
    }

    private static void validateMethodsToBeAutoDelegated(Element annotatedElement, TypeToExtend typeToExtend)
            throws ProcessingException {
        Set<ExecutableElement> methodsToBeDelegated = typeToExtend.getMethods();

        for (ExecutableElement methodElement : methodsToBeDelegated) {

            // methods annotated with DoNotDelegate have already been filtered out | TypeToExtend::interfaceMethodFilter

            if (methodElement.getModifiers().contains(Modifier.DEFAULT)
                    && methodElement.getAnnotation(DoDelegate.class) == null) {
                throw new ProcessingException(
                        annotatedElement,
                        "Default methods must be annotated with " + "either @DoNotDelegate or @DoDelegate");
            }
        }
    }

    private TypeToExtend createTypeToExtend(TypeElement annotatedElement) throws ProcessingException {
        PackageElement typePackage = elementUtils.getPackageOf(annotatedElement);

        if (typePackage.isUnnamed()) {
            throw new ProcessingException(annotatedElement, "Type %s doesn't have a package", annotatedElement);
        }

        if (annotatedElement.getModifiers().contains(Modifier.FINAL)) {
            throw new ProcessingException(annotatedElement, "Trying to extend final type %s", annotatedElement);
        }

        List<TypeElement> superTypes = fetchSuperinterfaces(annotatedElement);
        return new TypeToExtend(typePackage, annotatedElement, superTypes.toArray(new TypeElement[0]));
    }

    private List<TypeElement> fetchSuperinterfaces(TypeElement baseInterface) {
        List<TypeMirror> interfacesQueue = new ArrayList<>(baseInterface.getInterfaces());
        Set<TypeMirror> interfacesSet = new HashSet<>(interfacesQueue);
        List<TypeElement> superinterfaceElements = new ArrayList<>();

        for (int i = 0; i < interfacesQueue.size(); i++) {
            TypeMirror superinterfaceMirror = interfacesQueue.get(i);
            TypeElement superinterfaceType = ProcessorUtils.extractType(typeUtils, superinterfaceMirror);
            superinterfaceElements.add(superinterfaceType);

            List<TypeMirror> newInterfaces = superinterfaceType.getInterfaces().stream()
                    .filter(newInterface -> !interfacesSet.contains(newInterface))
                    .collect(Collectors.toList());
            interfacesSet.addAll(newInterfaces);
            interfacesQueue.addAll(newInterfaces);
        }

        return superinterfaceElements;
    }

    private void generateCode(TypeToExtend typeToExtend) throws IOException {
        String newTypeName = PREFIX + typeToExtend.getSimpleName();
        TypeSpec.Builder typeBuilder = TypeSpec.interfaceBuilder(newTypeName);
        typeBuilder.addTypeVariables(typeToExtend.getTypeParameterElements().stream()
                .map(TypeVariableName::get)
                .collect(Collectors.toList()));

        // Add modifiers
        TypeMirror typeMirror = typeToExtend.getType();
        if (typeToExtend.isPublic()) {
            typeBuilder.addModifiers(Modifier.PUBLIC);
        }

        typeBuilder.addSuperinterface(TypeName.get(typeMirror));

        // Add delegate method
        MethodSpec.Builder delegateMethod = MethodSpec.methodBuilder(DELEGATE_METHOD)
                .addModifiers(Modifier.PUBLIC, Modifier.ABSTRACT)
                .returns(TypeName.get(typeMirror));
        typeBuilder.addMethod(delegateMethod.build());

        // Add methods
        for (ExecutableElement methodElement : typeToExtend.getMethods()) {
            String returnStatement = (methodElement.getReturnType().getKind() == TypeKind.VOID) ? "" : "return ";

            int numParams = methodElement.getParameters().size();
            String callFormat = "$L$L().$L(" + joinMultiple("$L", numParams, ", ") + ")";
            Object[] callArgs = new Object[3 + methodElement.getParameters().size()];
            callArgs[0] = returnStatement;
            callArgs[1] = DELEGATE_METHOD;
            callArgs[2] = methodElement.getSimpleName();
            for (int i = 0; i < methodElement.getParameters().size(); ++i) {
                callArgs[i + 3] = methodElement.getParameters().get(i);
            }
            MethodSpec.Builder method = MethodSpec.overriding(methodElement).addStatement(callFormat, callArgs);
            method.addModifiers(Modifier.DEFAULT);

            typeBuilder.addMethod(method.build());
        }

        Goethe.formatAndEmit(
                JavaFile.builder(typeToExtend.getPackageName(), typeBuilder.build())
                        .build(),
                filer);
    }

    /**
     * Prints a warn message.
     *
     * @param element The element which has caused the error. Can be null
     * @param msg The error message
     */
    private void warn(Element element, String msg) {
        messager.printMessage(Diagnostic.Kind.WARNING, msg, element);
    }

    /**
     * Prints an error message.
     *
     * @param element The element which has caused the error. Can be null
     * @param msg The error message
     */
    private void error(Element element, String msg) {
        messager.printMessage(Diagnostic.Kind.ERROR, msg, element);
    }

    private static String joinMultiple(String string, int times, String delimiter) {
        return Stream.generate(() -> string).limit(times).collect(Collectors.joining(delimiter));
    }
}
