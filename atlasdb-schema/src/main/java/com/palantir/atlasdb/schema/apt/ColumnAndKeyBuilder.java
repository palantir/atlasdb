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
import java.util.Map;
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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.schema.annotations.Column;
import com.palantir.atlasdb.schema.annotations.FixedLength;


public class ColumnAndKeyBuilder {
	
	private final Map<String, FixedLength> fixedLengthKeys;
	private final List<ColumnDefinition> columns;
	private final Set<String> columnShortNames;
	private List<KeyDefinition> keys;
	
	public ColumnAndKeyBuilder(FixedLength[] fixedLengthColumns) {
		this.fixedLengthKeys = Maps.newHashMap();
		for(FixedLength fl : fixedLengthColumns) {
			this.fixedLengthKeys.put(fl.key(), fl);
		}
		
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
		List<KeyDefinition> keyDefinitions = getKeyDefinitions(methodElement);
		
		try {
			addColumn(keyDefinitions, columnName, shortName, columnTypeQualifiedName);
		} catch(IllegalArgumentException e) {
			throw new ProcessingException(methodElement, e.getMessage());
		}
	}

	public void addColumn(List<KeyDefinition> keys, String columnName, String columnShortName, String columnTypeQualifiedName) {
		// ensure short name does not clash
		if(columnShortNames.contains(columnShortName)) {
			throw new IllegalArgumentException("column short names must be unique - can specify via @Column(shortName = \"x\")");
		} else {
			columnShortNames.add(columnShortName);
		}
		
		// ensure keys match
		if(this.keys == null) {
			this.keys = keys;
		} else {
			ensureKeysAreConsistent(this.keys, keys);
		}
		
		ColumnDefinition cd = ImmutableColumnDefinition.builder()
				.columnName(columnName)
				.columnShortName(columnShortName)
				.columnTypeQualifiedName(columnTypeQualifiedName)
				.build();
		
		columns.add(cd);
	}
	
	
	private void ensureKeysAreConsistent(List<KeyDefinition> keys, List<KeyDefinition> otherKeys) {
		if(!Objects.equals(keys, otherKeys)) {
			throw new IllegalArgumentException("arguments to @Column getters must be consistently named and ordered");
		}
	}
	
	public ColumnsAndKeys build() {
		validateKeysAndColumnsNonEmpty();	
		validateNoUnusedFixedLengthDeclarations();	
		
		return ImmutableColumnsAndKeys.builder()
				.addAllColumnDefinitions(columns)
				.addAllKeys(keys)
				.build();
	}

	private void validateNoUnusedFixedLengthDeclarations() {
		// check for unused keys
		Set<String> keyIdentifiers = Sets.newHashSet();
		for(KeyDefinition kd : keys) {
			keyIdentifiers.add(kd.getName());
		}
		
		for(FixedLength fl : fixedLengthKeys.values()) {
			if(!keyIdentifiers.contains(fl.key())) {
				throw new IllegalStateException("have a @FixedLength annotation for " + fl.key() + " but it does not exist");
			}
		}
	}

	private void validateKeysAndColumnsNonEmpty() {
		// check for columns or keys being empty
		if(columns.isEmpty() || keys == null || keys.isEmpty()) {
			throw new IllegalStateException("must define at least one column, columns must have keys");
		}
	}
	
	private List<KeyDefinition> getKeyDefinitions(ExecutableElement element) throws ProcessingException {
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
				throw new ProcessingException(variableElement, "for now, only String or Long keys are supported");
			}
			
			KeyDefinition kd = makeKey(variableName, keyTypeQualifiedName);
			keys.add(kd);
		}
		return keys;
	}
	
	@VisibleForTesting
	KeyDefinition makeKey(String keyName, String keyTypeQualifiedName) {
		ImmutableKeyDefinition.Builder builder = ImmutableKeyDefinition.builder()
				.name(keyName)
				.keyTypeFullyQualified(keyTypeQualifiedName);
			
		if(fixedLengthKeys.containsKey(keyName)) {
			builder.isFixed(true).length(fixedLengthKeys.get(keyName).length());
		} else {
			builder.isFixed(false).length(-1);
		}
		
		return builder.build();		
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
