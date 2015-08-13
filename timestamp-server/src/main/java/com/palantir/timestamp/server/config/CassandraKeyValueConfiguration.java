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
