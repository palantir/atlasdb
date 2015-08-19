package com.palantir.atlasdb.keyvalue.remoting;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.Configuration;
import io.dropwizard.testing.DropwizardTestSupport;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class KeyValueServiceRemotingTest extends AbstractAtlasDbKeyValueServiceTest {

    final KeyValueService remoteKvs = RemotingKeyValueService.createServerSide(
            new InMemoryKeyValueService(false));

    @Rule
    public final DropwizardClientRule Rule = new DropwizardClientRule(
            remoteKvs,
            KeyAlreadyExistsExceptionMapper.instance(),
            InsufficientConsistencyExceptionMapper.instance());

    private final SimpleModule module = RemotingKeyValueService.kvsModule();
    private final ObjectMapper mapper = RemotingKeyValueService.kvsMapper();

    volatile KeyValueService localKvs;

    @SuppressWarnings("unchecked")
    @Before
    public void setupHacks() throws Exception {
        Field field = Rule.getClass().getDeclaredField("testSupport");
        field.setAccessible(true);
        DropwizardTestSupport<Configuration> testSupport = (DropwizardTestSupport<Configuration>) field.get(Rule);
        ObjectMapper mapper = testSupport.getEnvironment().getObjectMapper();
        mapper.registerModule(module);
        mapper.registerModule(new GuavaModule());
        testSupport.getApplication();
    }

    @Test
    public void testSerialize() throws IOException {
        Cell cell = Cell.create(row0, column0);
        String serializedCell = mapper.writeValueAsString(cell);
        System.err.println("serializedCell = " + serializedCell);
        Cell cellDeserialized = mapper.readValue(serializedCell, Cell.class);
        assertEquals(cell, cellDeserialized);

        byte[] row = row0;
        String serializedRow = mapper.writeValueAsString(row);
        System.err.println("serializedRow = " + serializedRow);
        byte[] rowDeserialized = mapper.readValue(serializedRow, byte[].class);
        Assert.assertArrayEquals(row, rowDeserialized);

        Map<Cell, byte[]> cellMap = ImmutableMap.of(cell, value00);
        String serializedMap =  mapper.writerFor(mapper.getTypeFactory().constructMapType(Map.class, Cell.class, byte[].class)).writeValueAsString(cellMap);
        System.err.println("serializedMap = " + serializedMap);
        Map<Cell, byte[]> cellMapDeserialized = mapper.readValue(serializedMap, mapper.getTypeFactory().constructMapType(Map.class, Cell.class, byte[].class));
        assertEquals(cellMap.size(), cellMapDeserialized.size());
        assertEquals(cellMap.keySet(), cellMapDeserialized.keySet());
        System.err.println("" + Arrays.toString(row) + " vs. " + Arrays.toString(cellMapDeserialized.values().iterator().next()));
        Assert.assertArrayEquals(value00, cellMapDeserialized.values().iterator().next());
    }

    @Test
    public void testBytes() throws IOException {
        String serializedEmptyByteArray = mapper.writeValueAsString(new byte[] {});
        byte[] readValue = mapper.readValue(serializedEmptyByteArray, byte[].class);
        Assert.assertArrayEquals(ArrayUtils.EMPTY_BYTE_ARRAY, readValue);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        if (localKvs == null) {
            String uri = Rule.baseUri().toString();
            localKvs = RemotingKeyValueService.createClientSide(Feign.builder()
                    .encoder(new JacksonEncoder(mapper))
                    .decoder(new EmptyOctetStreamDelegateDecoder(new JacksonDecoder(mapper)))
                    .contract(new JAXRSContract())
                    .target(KeyValueService.class, uri));
        }
        return Preconditions.checkNotNull(localKvs);
    }

}
