package com.palantir.timestamp.server;

import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.server.config.TimestampServerConfiguration;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class TimestampServer extends Application<TimestampServerConfiguration> {

    public static void main(String[] args) throws Exception {
        new TimestampServer().run(args);
    }

    @Override
    public void run(TimestampServerConfiguration configuration, Environment environment) throws Exception {
        environment.jersey().register(new InMemoryTimestampService());
    }


}
