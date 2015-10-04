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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.annotation.Annotation;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.schema.annotations.FixedLength;

public class ColumnAndKeyBuilderTest {
	private static final FixedLength key1FixedLength = new FixedLength() {
		@Override
		public Class<? extends Annotation> annotationType() {
			return FixedLength.class;
		}
		
		@Override
		public int length() {
			return 0;
		}
		
		@Override
		public String key() {
			return "key1";
		}
	};
	
	@Test
	public void weCannotBuildWithNoColumns() {
		ColumnAndKeyBuilder builder = new ColumnAndKeyBuilder(new FixedLength[] {});
		try {
			builder.build();
			fail();
		} catch(IllegalStateException e) {
			// pass
		}
	}
	
	@Test
	public void weCannotHaveUnusedAnnotations() {
		FixedLength[] fixedLengthKeys = new FixedLength[] { key1FixedLength };
		
		KeyDefinition kd = ImmutableKeyDefinition.builder()
				.name("key2")
				.keyTypeFullyQualified("java.lang.String")
				.isFixed(false)
				.length(10)
				.build();
		
		ColumnAndKeyBuilder builder = new ColumnAndKeyBuilder(fixedLengthKeys);
		builder.addColumn(ImmutableList.of(kd), "columnName", "c", "java.lang.String");
		try {
			builder.build();
			fail();
		} catch(IllegalStateException e) {
			// pass
		}
	}
	
	@Test
	public void aFixedLengthAnnotationGetsPickedUp() {
		FixedLength[] fixedLengthKeys = new FixedLength[] { key1FixedLength };

		ColumnAndKeyBuilder builder = new ColumnAndKeyBuilder(fixedLengthKeys);

		KeyDefinition key1 = builder.makeKey("key1", "java.lang.String");
		assertTrue(key1.isFixed());
		
		KeyDefinition key2 = builder.makeKey("key2", "java.lang.String");
		assertFalse(key2.isFixed());
	}
}
