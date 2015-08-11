package com.palantir.server;

import java.util.Set;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlas.api.AtlasService;
import com.palantir.atlas.impl.AtlasServiceImpl;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.memory.InMemoryAtlasDb;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.UpgradeSchema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.timestamp.server.TimestampServer;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class TransactionRemotingTest {
    public final static AtlasSchema schema = UpgradeSchema.INSTANCE;
    public final static SerializableTransactionManager txMgr = InMemoryAtlasDb.createInMemoryTransactionManager(schema);
    public final static KeyValueService kvs = txMgr.getKeyValueService();

    @ClassRule
    public final static DropwizardClientRule dropwizard = new DropwizardClientRule(new AtlasServiceImpl(kvs, txMgr, new TableMetadataCache(kvs)));

    @Test
    public void testSerializing() {
        ObjectMapper mapper = TimestampServer.getObjectMapper();

        String uri = dropwizard.baseUri().toString();
        AtlasService atlasdb = Feign.builder()
                .decoder(new JacksonDecoder(mapper))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .target(AtlasService.class, uri);

        Set<String> tables = atlasdb.getAllTableNames();

    }
}
