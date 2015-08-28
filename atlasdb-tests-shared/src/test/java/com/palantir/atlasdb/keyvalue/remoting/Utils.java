package com.palantir.atlasdb.keyvalue.remoting;

import java.lang.reflect.Field;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.server.InboxPopulatingContainerRequestFilter;
import com.palantir.common.base.Throwables;

import io.dropwizard.Configuration;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class Utils {

    public static final SimpleModule module = RemotingKeyValueService.kvsModule();
    public static final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    public static DropwizardClientRule getRemoteKvsRule(KeyValueService remoteKvs) {
        DropwizardClientRule rule = new DropwizardClientRule(remoteKvs,
                KeyAlreadyExistsExceptionMapper.instance(),
                InsufficientConsistencyExceptionMapper.instance(),
                VersionTooOldExceptionMapper.instance(),
                new InboxPopulatingContainerRequestFilter(mapper));
        return rule;
    }

    public static void setupRuleHacks(DropwizardClientRule rule) {
        try {
            Field field = rule.getClass().getDeclaredField("testSupport");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            DropwizardTestSupport<Configuration> testSupport = (DropwizardTestSupport<Configuration>) field.get(rule);
            ObjectMapper mapper = testSupport.getEnvironment().getObjectMapper();
            mapper.registerModule(Utils.module);
            mapper.registerModule(new GuavaModule());
            testSupport.getApplication();
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

}