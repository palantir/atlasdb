package com.palantir.atlas.impl;

import javax.inject.Singleton;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlas.api.AtlasService;
import com.palantir.atlas.jackson.AtlasJacksonModule;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.schema.UpgradeFailedException;
import com.palantir.atlasdb.transaction.api.TransactionManager;

public class AtlasServerModule implements PalantirModule {

    @Bind @Singleton DatabaseContext bindDatabaseContext() {
        DatabaseContext dbContext = DatabaseContextImpl.createDatabaseContext();
        dbContext.getDbMgr().initSystemProperties(DBConstants.DISPATCH_SCHEMA_TYPE);
        return dbContext;
    }

    @Bind @Singleton AtlasConnector bindAtlasConnector(DatabaseContext dbContext) throws UpgradeFailedException {
        AtlasModuleSupplier modules = new AtlasModuleSupplierBuilder()
            .txManagerModule(HiddenTableTxManagerModule.class)
            .build();
        AtlasConnector connector = new AtlasConnector(dbContext, MetropolisPR.getConfig(dbContext), modules.get());
        MetropolisPR.setConnector(dbContext, connector);
        connector.initializeAtlas();
        return connector;
    }

    @Bind KeyValueService bindKeyValueService(AtlasConnector connector) {
        return connector.getKeyValueService();
    }

    @Bind TransactionManager bindTransactionManager(AtlasConnector connector) {
        return connector.getTxManager();
    }

    @Bind AtlasService bind(AtlasServiceImpl impl) {
        return impl;
    }

    @Bind @Singleton ObjectMapper bindObjectMapper(AtlasJacksonModule module) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(module.createModule());
        return mapper;
    }
}
