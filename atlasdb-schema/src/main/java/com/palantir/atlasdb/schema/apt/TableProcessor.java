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

import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.Messager;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.PackageElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.DisplayTool;
import org.jboss.forge.roaster.Roaster;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.palantir.atlasdb.schema.annotations.FixedLength;
import com.palantir.atlasdb.schema.annotations.Keys;
import com.palantir.atlasdb.schema.annotations.Table;
import com.palantir.atlasdb.schema.apt.ColumnAndKeyBuilder.ColumnsAndKeys;

public class TableProcessor extends AbstractProcessor {

	private Types typeUtils;
	private Elements elementUtils;
	private Filer filer;
	private Messager messager;

	@Override
	public synchronized void init(ProcessingEnvironment processingEnv) {
		super.init(processingEnv);
		typeUtils = processingEnv.getTypeUtils();
		elementUtils = processingEnv.getElementUtils();
		filer = processingEnv.getFiler();
		messager = processingEnv.getMessager();
	}

	@Override
	public Set<String> getSupportedAnnotationTypes() {
		Set<String> annotations = ImmutableSet.of(Table.class.getCanonicalName());
		return annotations;
	}

	@Override
	public SourceVersion getSupportedSourceVersion() {
		return SourceVersion.latestSupported();
	}

	@Override
	public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
		Multimap<String, AtlasTableDefinition> tablesBySchema = ArrayListMultimap.create();
		
		try {
			for (Element annotatedElement : roundEnv.getElementsAnnotatedWith(Table.class)) {
				if (annotatedElement.getKind() != ElementKind.INTERFACE) {
					throw new ProcessingException(annotatedElement, "Only interfaces can be annotated with @%s",
							Table.class.getSimpleName());
				}
				
				AtlasTableDefinition tableDefinition = processTable((TypeElement) annotatedElement);
				tablesBySchema.put(tableDefinition.getSchemaName(), tableDefinition);
			}
		} catch (ProcessingException e) {
			error(e.getElement(), e.getMessage());
		}
		
		for(String schemaName : tablesBySchema.keySet()) {
			Collection<AtlasTableDefinition> tables = tablesBySchema.get(schemaName);
			
			ImmutableSchemaDefinition.Builder schemaBuilder = ImmutableSchemaDefinition.builder();
			schemaBuilder.schemaName(schemaName);
			
			for(AtlasTableDefinition tableDefinition : tables) {
				try {
					writeTableSource(tableDefinition);
					schemaBuilder.addTableDefinitions(tableDefinition);
				} catch (Exception e) {
					error(null, "could not write output source: " + e.getMessage());
				}
			}
			schemaBuilder.packageName(tables.iterator().next().getPackageName()); // just grab first package name
			String className = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, schemaName + "_schema");
			schemaBuilder.generatedClassName(className);
			
			SchemaDefinition schemaDefinition = schemaBuilder.build();
			try {
				writeSchemaSource(schemaDefinition);
			} catch(Exception e) {
				error(null, "could not write output source: " + e.getMessage());
			}
		}

		return true;
	}
	
	private AtlasTableDefinition processTable(TypeElement typeElement) throws ProcessingException {
		
		String originalClassName = typeElement.getSimpleName().toString();
		PackageElement packageElement = elementUtils.getPackageOf(typeElement);
		if(packageElement.isUnnamed()) {
			throw new ProcessingException(typeElement, originalClassName + " cannot be in the default package");
		}
	    String packageName = packageElement.getQualifiedName().toString();
		String generatedClassName = originalClassName + "Implementation"; // TODO (bduffield)
		
		Table table = typeElement.getAnnotation(Table.class);
		String tableName = table.name();
		String schemaName = table.schemaName();
		if(!schemaName.matches("^[a-z][a-z_]*$")) {
			throw new ProcessingException(typeElement, "schema name '" + schemaName + "' is not in underscore_case");
		}
		
		// now process all of the columns
		ColumnAndKeyBuilder.ColumnsAndKeys columnsAndKeys = getColumnDefinitions(typeElement);
		
		return ImmutableAtlasTableDefinition.builder()
				.originalClassName(originalClassName)
				.packageName(packageName)
				.generatedClassName(generatedClassName)
				.tableName(tableName)
				.schemaName(schemaName)
				.addAllColumnDefinitions(columnsAndKeys.getColumnDefinitions())
				.addAllKeyDefinitions(columnsAndKeys.getKeys())
				.build();
	}
	
	private ColumnsAndKeys getColumnDefinitions(Element element) throws ProcessingException {
		Keys keys = element.getAnnotation(Keys.class);
		FixedLength[] fixedLengthKeys = keys != null ? keys.value() : new FixedLength[] {};
		
		List<? extends Element> allElements = element.getEnclosedElements(); 
		ColumnAndKeyBuilder builder = new ColumnAndKeyBuilder(fixedLengthKeys);
		
		for(Element e : allElements) {
			if(e.getKind() != ElementKind.METHOD) {
				continue;
			}
			
			builder.addColumn((ExecutableElement) e);
		}
		
		try {
			return builder.build();
		} catch(IllegalStateException e) {
			throw new ProcessingException(element, e.getMessage());
		}
	}
	
	private void writeTableSource(AtlasTableDefinition tableDefinition) throws Exception {
		JavaFileObject jfo = processingEnv.getFiler()
				.createSourceFile(tableDefinition.getPackageName() + "." + tableDefinition.getGeneratedClassName());
		messager.printMessage(Diagnostic.Kind.NOTE, "creating source file: " + jfo.toUri());

		try(Writer writer = jfo.openWriter()) {
			writeTableSource(tableDefinition, writer);			
		}
	}
	
	private void writeSchemaSource(SchemaDefinition schemaDefinition) throws Exception {
		JavaFileObject jfo = processingEnv.getFiler()
				.createSourceFile(schemaDefinition.getPackageName() + "." + schemaDefinition.getGeneratedClassName());
		messager.printMessage(Diagnostic.Kind.NOTE, "creating source file: " + jfo.toUri());

		try(Writer writer = jfo.openWriter()) {			
			writeSchemaSource(schemaDefinition, writer);			
		}
	}
	
	private static void writeTableSource(AtlasTableDefinition tableDefinition, Writer writer) throws Exception {
		VelocityEngine ve = makeAndInitializeVelocityEngine();
		Template tableTemplate = ve.getTemplate("table.vm");

		VelocityContext tableContext = new VelocityContext();
		tableContext.put("tableDefinition", tableDefinition);
		tableContext.put("display", new DisplayTool());
		
		StringWriter stringWriter = new StringWriter();
		tableTemplate.merge(tableContext, stringWriter);
		
		String codeAsString = stringWriter.toString();
		String formatted = Roaster.format(codeAsString);	
		writer.write(formatted);
	}
	
	private static void writeSchemaSource(SchemaDefinition schemaDefinition, Writer writer) throws Exception {
		VelocityEngine ve = makeAndInitializeVelocityEngine();
		Template schemaTemplate = ve.getTemplate("schema.vm");

		VelocityContext tableContext = new VelocityContext();
		tableContext.put("schemaDefinition", schemaDefinition);
		tableContext.put("display", new DisplayTool());
		
		StringWriter stringWriter = new StringWriter();
		schemaTemplate.merge(tableContext, stringWriter);
		
		String codeAsString = stringWriter.toString();
		String formatted = Roaster.format(codeAsString);	
		writer.write(formatted); 
	}
	
	private static VelocityEngine makeAndInitializeVelocityEngine() throws Exception {
		Properties props = new Properties();
		URL url = TableProcessor.class.getClassLoader().getResource("velocity.properties");
		props.load(url.openStream());

		VelocityEngine ve = new VelocityEngine(props);
		ve.init();
		
		return ve;
	}

	private void error(Element e, String msg) {
		messager.printMessage(Diagnostic.Kind.ERROR, msg, e);
	}
}