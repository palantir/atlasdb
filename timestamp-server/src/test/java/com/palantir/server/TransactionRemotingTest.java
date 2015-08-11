package com.palantir.server;

import java.io.IOException;
import java.io.InputStream;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.impl.AtlasServiceImpl;
import com.palantir.atlas.impl.TableMetadataCache;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.memory.InMemoryAtlasDb;
import com.palantir.atlasdb.schema.AtlasSchema;
import com.palantir.atlasdb.schema.UpgradeSchema;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.timestamp.server.TimestampServer;

import io.dropwizard.testing.junit.ResourceTestRule;

public class TransactionRemotingTest {
    public final static AtlasSchema schema = UpgradeSchema.INSTANCE;
    public final static SerializableTransactionManager txMgr = InMemoryAtlasDb.createInMemoryTransactionManager(schema);
    public final static KeyValueService kvs = txMgr.getKeyValueService();
    public final static TableMetadataCache cache = new TableMetadataCache(kvs);
    public final static ObjectMapper mapper = TimestampServer.getObjectMapper(cache);

    @ClassRule
    public static final ResourceTestRule resources = ResourceTestRule.builder()
            .addResource(new AtlasServiceImpl(kvs, txMgr, cache))
            .setMapper(mapper)
            .build();


    @Test
    public void testSerializing() throws IOException {
        Response response = resources.client().target("/atlasdb/metadata/upgrade_metadata").request().get();
        String str = IOUtils.toString(response.readEntity(InputStream.class));

        response = resources.client().target("/atlasdb/transaction").request().post(Entity.entity("", MediaType.TEXT_PLAIN));
        str = IOUtils.toString(response.readEntity(InputStream.class));
        long token = Long.parseLong(str);

        TableRange tableRange = new TableRange("upgrade_metadata", PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, ImmutableList.<byte[]>of(), 100);
        String rangeStr = mapper.writeValueAsString(tableRange);
        response = resources.client().target("/atlasdb/range/" + token).request().post(Entity.entity(rangeStr, MediaType.APPLICATION_JSON));
        str = IOUtils.toString(response.readEntity(InputStream.class));
        System.out.println(str);
    }

}
