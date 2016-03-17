package com.palantir.atlasdb.cli.services;

import static com.palantir.atlasdb.factory.TransactionManagers.LockAndTimestampServices;
import static com.palantir.atlasdb.factory.TransactionManagers.createLockAndTimestampServices;

import javax.inject.Named;
import javax.inject.Singleton;

import com.palantir.atlasdb.factory.ImmutableLockAndTimestampServices;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
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
    public LockAndTimestampServices provideLockAndTimestampServices(ServicesConfig config, @Named("rawKvs") KeyValueService rawKvs) {
        LockAndTimestampServices lts = createLockAndTimestampServices(
                config.atlasDbConfig(),
                config.sslSocketFactory(),
                resource -> {},
                LockServiceImpl::create,
                () -> config.atlasDbFactory().createTimestampService(rawKvs));
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
