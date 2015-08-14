package com.palantir.timestamp.server.config;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.google.common.collect.ImmutableList;

public class LeaderConfiguration {
	@NotNull
	@Valid
	public int quorumSize = 1;

	@NotNull
	@Valid
	public boolean promote = false;

	@NotNull
	@Valid
	public String localServer = "http://localhost:3828";

	@NotNull
	@Valid
	public List<String> leaders = ImmutableList.of("http://localhost:3828");

	@NotNull
	@Valid
	public String learnerLogDir = "paxosLog/learner";

	@NotNull
	@Valid
	public String acceptorLogDir = "paxosLog/acceptor";

}
