package com.palantir.atlasdb.keyvalue.remoting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlasdb.keyvalue.partition.PartitionMapService;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;

public class RemotingPartitionMapService {

    final static ObjectMapper mapper = new ObjectMapper();

    public static PartitionMapService createClientSide(String uri) {
        return Feign.builder()
                .decoder(new JacksonDecoder())
                .encoder(new JacksonEncoder())
                .contract(new JAXRSContract())
                .target(PartitionMapService.class, uri);
    }

}
