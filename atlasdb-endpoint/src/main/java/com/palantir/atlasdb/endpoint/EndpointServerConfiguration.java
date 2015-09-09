package com.palantir.atlasdb.endpoint;

import com.fasterxml.jackson.databind.JsonNode;

import io.dropwizard.Configuration;

public class EndpointServerConfiguration extends Configuration {

    public JsonNode extraConfig;

}
