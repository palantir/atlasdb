/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.schema.apt;

import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.schema.annotations.Keys;
import com.palantir.atlasdb.schema.annotations.Table;

/**
 * Check that keys annotations are used in the correct place!
 */
public class KeysAnnotationProcessor extends AbstractProcessor {
	
	private Messager messager;

	@Override
	public synchronized void init(ProcessingEnvironment processingEnv) {
		super.init(processingEnv);
		messager = processingEnv.getMessager();
	}
	
	
	@Override
	public Set<String> getSupportedAnnotationTypes() {
		Set<String> annotations = ImmutableSet.of(Keys.class.getCanonicalName());
		return annotations;
	}
	
	@Override
	public SourceVersion getSupportedSourceVersion() {
		return SourceVersion.latestSupported();
	}

	@Override
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
		try {
			for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(Keys.class)) {
				if (annotatedElement.getKind() != ElementKind.INTERFACE || 
						annotatedElement.getAnnotation(Table.class) == null) {
					throw new ProcessingException(annotatedElement, "The @Keys annotation must be on an interface also annotated with @Table");
				}
			}
		} catch (ProcessingException e) {
			error(e.getElement(), e.getMessage());
		}

		return true;
	}

	private void error(Element e, String msg) {
		messager.printMessage(Diagnostic.Kind.ERROR, msg, e);
	}
}
