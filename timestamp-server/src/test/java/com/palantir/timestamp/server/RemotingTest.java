package com.palantir.timestamp.server;

import org.junit.ClassRule;

import com.palantir.timestamp.server.config.TimestampServerConfiguration;

import io.dropwizard.testing.junit.DropwizardAppRule;

public class RemotingTest {
    @ClassRule
    public static final DropwizardAppRule<TimestampServerConfiguration> APP_RULE = new DropwizardAppRule<>(TimestampServer.class, "src/test/resources/testService.yml");


}
