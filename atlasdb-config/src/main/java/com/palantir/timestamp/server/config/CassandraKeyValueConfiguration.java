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
package com.palantir.timestamp.server.config;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class CassandraKeyValueConfiguration {
	@NotNull
	@Valid
    public List<String> servers = null;
	@NotNull
	@Valid
    public int port = 9160;
	@NotNull
	@Valid
    public int poolSize = 20;
	@NotNull
	@Valid
    public String keyspace = "atlasdb";
	@NotNull
	@Valid
    public boolean isSsl = false;
	@NotNull
	@Valid
    public int replicationFactor = 3;
	@NotNull
	@Valid
    public int mutationBatchCount = 5000;
	@NotNull
	@Valid
    public int mutationBatchSizeBytes = 4 * 1024 * 1024;
	@NotNull
	@Valid
    public int fetchBatchCount = 5000;
	@NotNull
	@Valid
    public boolean safetyDisabled = false;
	@NotNull
	@Valid
    public boolean autoRefreshNodes = true;
}
