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

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ascii;
import com.google.common.base.CaseFormat;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.schema.annotations.Column;


public class ColumnAndKeyBuilder {
	
	private List<KeyDefinition> keys;
	private List<ColumnDefinition> columns;
	private Set<String> columnShortNames;
	
	public ColumnAndKeyBuilder() {
		this.columns = Lists.newArrayList();
		this.columnShortNames = Sets.newHashSet();
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
		
		// add keys or ensure keys are consistent
		if(this.keys == null) {
			this.keys = getKeyDefinitions(methodElement);
		} else {
			ensureKeysAreConsistent(methodElement);
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
	
	public ColumnsAndKeys build() {
		if(columns.isEmpty() || keys == null || keys.isEmpty()) {
			throw new IllegalStateException();
		}
		
		return ImmutableColumnsAndKeys.builder()
				.addAllColumnDefinitions(columns)
				.addAllKeys(keys)
				.build();
	}
	
	private void ensureKeysAreConsistent(ExecutableElement element) throws ProcessingException {
		// the keys must match
		List<KeyDefinition> keysForElement = getKeyDefinitions(element);
		if(!Objects.equals(this.keys, keysForElement)) {
			throw new ProcessingException(element, "arguments to @Column getters must be consistently named and ordered");
		}
	}
	
	private static List<KeyDefinition> getKeyDefinitions(ExecutableElement element) throws ProcessingException {
		List<KeyDefinition> keys = Lists.newArrayList();
		for(VariableElement variableElement : element.getParameters()) {
			String variableName = variableElement.getSimpleName().toString();
			
			TypeMirror variableType = variableElement.asType();
			if(!(variableType instanceof DeclaredType)) {
				throw new ProcessingException(variableElement, "for now, keys must be non-primitives");
			}
			
			DeclaredType declaredType = (DeclaredType) variableType;
			TypeElement typeElement = (TypeElement) declaredType.asElement();
			String keyTypeQualifiedName = typeElement.getQualifiedName().toString();
			
			switch(keyTypeQualifiedName) {
			case "java.lang.String":
			case "java.lang.Long":
				break;
			default:
				throw new ProcessingException(variableElement, "for now, only String or Long (variable-length encoded) keys are supported");
			}
			
			ImmutableKeyDefinition key = ImmutableKeyDefinition.builder()
				.name(variableName)
				.keyTypeFullyQualified(keyTypeQualifiedName)
				.build();
			keys.add(key);
		}
		return keys;
	}

	private static String getColumnNameFromMethod(ExecutableElement element) throws ProcessingException {
		String methodName = element.getSimpleName().toString();
		try {
			return getColumnNameFromMethodName(methodName);
		} catch(IllegalArgumentException e) {
			throw new ProcessingException(element, "methods annotated with @Column must be getters");
		}
	}

	@VisibleForTesting
	protected static String getColumnNameFromMethodName(String methodName) {
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
	
	@Value.Immutable
	static interface ColumnsAndKeys {
		List<ColumnDefinition> getColumnDefinitions();
		List<KeyDefinition> getKeys();
	}
}
