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

import com.google.common.collect.ImmutableList;

import io.dropwizard.Configuration;

public class TimestampServerConfiguration extends Configuration {
	@NotNull
	@Valid
	public int quorumSize = 1;

	@NotNull
	@Valid
	public int localIndex = 0;
	@NotNull
	@Valid
	public List<String> servers = ImmutableList.of("localhost:1234");

	@NotNull
	@Valid
	public String learnerLogDir = "paxosLog/learner";

	@NotNull
	@Valid
	public String acceptorLogDir = "paxosLog/acceptor";
}
