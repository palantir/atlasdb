package com.palantir.atlasdb.cli.services;

import static com.palantir.atlasdb.factory.TransactionManagers.LockAndTimestampServices;
import static com.palantir.atlasdb.factory.TransactionManagers.createLockAndTimestampServices;

import javax.inject.Named;
import javax.inject.Singleton;
import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.factory.ImmutableLockAndTimestampServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.lock.RemoteLockService;
import com.palantir.lock.client.LockRefreshingRemoteLockService;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.timestamp.TimestampService;

import dagger.Module;
import dagger.Provides;

@Module
public class LockAndTimestampModule {

    @Provides
    @Singleton
    public LockAndTimestampServices provideLockAndTimestampServices(AtlasDbConfig config, @Named("rawKvs") KeyValueService rawKvs, AtlasDbFactory kvsFactory, Optional<SSLSocketFactory> sslSocketFactory) {
        LockAndTimestampServices lts = createLockAndTimestampServices(
                config,
                sslSocketFactory,
                resource -> {
                },
                LockServiceImpl::create,
                () -> kvsFactory.createTimestampService(rawKvs));
        return ImmutableLockAndTimestampServices.builder()
                .from(lts)
                .lock(LockRefreshingRemoteLockService.create(lts.lock()))
                .build();

    }

    @Provides
    @Singleton
    public TimestampService provideTimestampService(LockAndTimestampServices lts) {
        return lts.time();
    }

    @Provides
    @Singleton
    public RemoteLockService provideLockService(LockAndTimestampServices lts) {
        return lts.lock();
    }

}
