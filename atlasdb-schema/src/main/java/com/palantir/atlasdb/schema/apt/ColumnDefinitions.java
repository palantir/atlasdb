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

import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import com.google.common.base.Ascii;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.schema.annotations.Column;


public class ColumnDefinitions {
	
	private List<String> keys;
	private List<ColumnDefinition> columns;
	private Set<String> columnShortNames;
	
	public ColumnDefinitions() {
		this.columns = Lists.newArrayList();
		this.columnShortNames = Sets.newHashSet();
	}
	
	public List<ColumnDefinition> getColumns() {
		return columns;
	}
	
	public List<String> getKeys() {
		return keys;
	}
	
	public void addColumn(ExecutableElement methodElement) throws ProcessingException {
		Column columnAnnotation = methodElement.getAnnotation(Column.class);
		if(columnAnnotation == null) {
			// this is some other method
			return;
		}
			
		String columnName = getColumnNameFromMethod(methodElement);
		String shortName = columnAnnotation.shortName().length() > 0 ?
				columnAnnotation.shortName() : columnName.substring(0,1);
			
		List<String> keys = Lists.newArrayList();
		for(VariableElement variableElement : methodElement.getParameters()) {
			String variableName = variableElement.getSimpleName().toString();
			keys.add(variableName);
		}
		
		TypeMirror returnType = methodElement.getReturnType();
		if(returnType.getKind() == TypeKind.VOID) {
			throw new ProcessingException(methodElement, "@Column getters must have a return type");
		}
		if(!(returnType instanceof DeclaredType)) {
			throw new ProcessingException(methodElement, "@Column currently only handles non-primitives");
		}
		
		DeclaredType declaredType = (DeclaredType) returnType;
		TypeElement typeElement = (TypeElement) declaredType.asElement();
		String columnTypeQualifiedName = typeElement.getQualifiedName().toString();
		
		// ensure keys are consistent
		if(this.keys == null) {
			this.keys = keys;
		} else if(!Objects.equals(this.keys, keys)) {
			throw new ProcessingException(methodElement, "arguments to @Column getters must be consistently named and ordered");
		}
		
		// ensure short name does not clash
		if(columnShortNames.contains(shortName)) {
			throw new ProcessingException(methodElement, "column short names must be unique - can specify via @Column(shortName = \"x\")");
		} else {
			columnShortNames.add(shortName);
		}
		
		ColumnDefinition cd = ImmutableColumnDefinition.builder()
				.columnName(columnName)
				.columnShortName(shortName)
				.columnTypeQualifiedName(columnTypeQualifiedName)
				.build();
		
		columns.add(cd);
	}

	private static String getColumnNameFromMethod(ExecutableElement element) throws ProcessingException {
		String methodName = element.getSimpleName().toString();
		try {
			return getColumnNameFromMethodName(methodName);
		} catch(IllegalArgumentException e) {
			throw new ProcessingException(element, "methods annotated with @Column must be getters");
		}
	}

	public static String getColumnNameFromMethodName(String methodName) {
		// this is how immutables does it
		final String prefix = "get";
		boolean prefixMatches = methodName.startsWith(prefix) && Ascii.isUpperCase(methodName.charAt(prefix.length()));
		
		if (prefixMatches) {
			String columnName = methodName.substring(prefix.length(), methodName.length());
			return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, columnName);
		} else {
			throw new IllegalArgumentException();
		}
	}
}
