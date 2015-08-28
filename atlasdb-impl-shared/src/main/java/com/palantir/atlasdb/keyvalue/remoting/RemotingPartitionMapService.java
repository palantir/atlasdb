package com.palantir.atlasdb.keyvalue.remoting;

import com.palantir.atlasdb.keyvalue.partition.map.PartitionMapService;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;

public class RemotingPartitionMapService {

    public static PartitionMapService createClientSide(String uri) {
        return Feign.builder()
                .decoder(new JacksonDecoder(RemotingKeyValueService.kvsMapper()))
                .encoder(new JacksonEncoder(RemotingKeyValueService.kvsMapper()))
                .contract(new JAXRSContract())
                .target(PartitionMapService.class, uri);
    }

}
