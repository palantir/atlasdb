package com.palantir.timestamp.server.config;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.google.common.collect.ImmutableList;

public class ClientConfiguration {
	@NotNull
	@Valid
	public List<String> servers = ImmutableList.of("http://localhost:3828");

}
