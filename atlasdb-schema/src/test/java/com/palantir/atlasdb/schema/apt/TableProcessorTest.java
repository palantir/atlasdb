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

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

import java.net.MalformedURLException;

import javax.tools.JavaFileObject;

import org.junit.Test;

import com.google.testing.compile.JavaFileObjects;

public class TableProcessorTest {
	@Test
	public void theProcessorFailsWhenWeAnnotateAClass() throws MalformedURLException {
		JavaFileObject fileObject = JavaFileObjects.forResource("com/palantir/atlasdb/schema/test/AnnotationOnClass.java");
		assert_().about(javaSource())
		    .that(fileObject)
		    .processedWith(new org.immutables.value.internal.$processor$.$Processor(), new TableProcessor())
		    .failsToCompile()
		    .withErrorContaining("Only interfaces can be annotated with @Table");
	}
	
	@Test
	public void theProcessorFailsWhenWeDefineNoColumns() throws MalformedURLException {
		JavaFileObject fileObject = JavaFileObjects.forResource("com/palantir/atlasdb/schema/test/NoColumns.java");
		assert_().about(javaSource())
		    .that(fileObject)
		    .processedWith(new org.immutables.value.internal.$processor$.$Processor(), new TableProcessor())
		    .failsToCompile()
		    .withErrorContaining("must define at least one column");
	}
	
	@Test
	public void theProcessorFailsWhenWeUseKeysInconsistently() throws MalformedURLException {
		JavaFileObject fileObject = JavaFileObjects.forResource("com/palantir/atlasdb/schema/test/InconsistentKeyUsage.java");
		assert_().about(javaSource())
		    .that(fileObject)
		    .processedWith(new org.immutables.value.internal.$processor$.$Processor(), new TableProcessor())
		    .failsToCompile()
		    .withErrorContaining("arguments to @Column getters must be consistently named and ordered");
	}
	
	@Test
	public void weCanProcessABasicAnnotatedInterface() {
		JavaFileObject fileObject = JavaFileObjects.forResource("com/palantir/atlasdb/schema/test/BasicTable.java");
		assert_().about(javaSource())
		    .that(fileObject)
		    .processedWith(new org.immutables.value.internal.$processor$.$Processor(), new TableProcessor())
		    .compilesWithoutError();
	}
	
	@Test
	public void weProduceGeneratedOutputForABasicTableWithFixedLengthKeys() {
		JavaFileObject fileObject = JavaFileObjects.forResource("com/palantir/atlasdb/schema/test/BasicTableWithFixedLength.java");
		assert_().about(javaSource())
		    .that(fileObject)
		    .processedWith(new org.immutables.value.internal.$processor$.$Processor(), new TableProcessor())
		    .compilesWithoutError();
	}
	
	@Test
	public void weErrorForABasicTableWithIncorrectFixedLengthKeys() {
		JavaFileObject fileObject = JavaFileObjects.forResource("com/palantir/atlasdb/schema/test/BasicTableWithNonExistingFixedLength.java");
		assert_().about(javaSource())
		    .that(fileObject)
		    .processedWith(new org.immutables.value.internal.$processor$.$Processor(), new TableProcessor())
		    .failsToCompile()
		    .withErrorContaining("have a @FixedLength annotation for key3 but it does not exist"); 
	}
}
