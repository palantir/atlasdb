package com.palantir.atlasdb.rdbms.impl.service;

import java.util.Collection;
import java.util.Collections;

import com.google.common.collect.Lists;
import com.palantir.atlasdb.rdbms.api.config.AtlasRdbmsConfiguration;
import com.palantir.atlasdb.rdbms.impl.util.TempTableDescriptor;
import com.palantir.atlasdb.rdbms.impl.util.TempTableDescriptorProvider;

public class AtlasRdbmsTestUtils {

    public static AtlasRdbmsConfiguration getTestConfig() {
        return new AtlasRdbmsConfiguration("jdbc:h2:mem:atlas_rdbms_test;DB_CLOSE_DELAY=-1", "org.h2.Driver",
                16, "atlas_system_properties");
    }

    public static TempTableDescriptorProvider getTestTempTables() {
        return new TempTableDescriptorProvider() {
            @Override
            public Collection<TempTableDescriptor> getTempTableDescriptors() {
                TempTableDescriptor descriptor = new TempTableDescriptor("atlas_temp_ids",
                        Collections.singletonMap("id", "BIGINT"));
                return Lists.newArrayList(descriptor);
            }
        };
    }

    public static AtlasRdbmsImpl getTestDb() {
        return AtlasRdbmsImpl.create(getTestConfig(), getTestTempTables());
    }
}
