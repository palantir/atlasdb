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
package com.palantir.atlasdb.schema.tests;

import com.palantir.atlasdb.schema.annotations.Column;
import com.palantir.atlasdb.schema.annotations.FixedLength;
import com.palantir.atlasdb.schema.annotations.Keys;
import com.palantir.atlasdb.schema.annotations.Table;

@Table(name = "my_test_table")
@Keys({
	@FixedLength(length = 10, key = "newFixedThing")
})
public interface TestTable {

	@Column
	String getGraphName(Long blahId, String newFixedThing, String nodeId);
	
	@Column
	Integer getNodeName(Long blahId, String newFixedThing, String nodeId);
	
	@Column(shortName="x")
	String getGeneralNodeName(Long blahId, String newFixedThing, String nodeId);
}
