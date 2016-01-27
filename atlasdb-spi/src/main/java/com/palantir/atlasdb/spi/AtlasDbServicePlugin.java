package com.palantir.atlasdb.spi;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

/**
 * This plugin point is used to expose endpoints in the atlas service context.
 */
@JsonTypeInfo(use = Id.NAME, include = As.PROPERTY, property = "type", visible = false)
public interface AtlasDbServicePlugin {
    String type();

    void registerServices(AtlasDbServerEnvironment env);
}
