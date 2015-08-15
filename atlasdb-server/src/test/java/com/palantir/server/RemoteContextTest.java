package com.palantir.server;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;

import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.palantir.common.base.Throwables;
import com.palantir.common.supplier.AbstractWritableServiceContext;
import com.palantir.common.supplier.PopulateServiceContextProxy;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.RemoteContextHolder.RemoteContextType;
import com.palantir.common.supplier.ServiceContext;

import feign.Feign;
import feign.RequestInterceptor;
import feign.RequestTemplate;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import io.dropwizard.testing.junit.DropwizardClientRule;

public class RemoteContextTest {
    @ClassRule
    public final static DropwizardClientRule dropwizard = new DropwizardClientRule(
            new InboxResourceImpl(),
            new InboxPopulatingContainerRequestFilter());

    @Test
    public void testSerializing() {
        ObjectMapper mapper = new ObjectMapper();
        String uri = dropwizard.baseUri().toString();
        InboxResource ir = Feign.builder()
                .decoder(new JacksonDecoder(mapper))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .requestInterceptor(new OutboxShippingInterceptor())
                .target(InboxResource.class, uri);
        ServiceContext<String> context = RemoteContextHolder.OUTBOX.getProviderForKey(HOLDER.VALUE);
        ir = PopulateServiceContextProxy.newProxyInstanceWithConstantValue(InboxResource.class, ir, "whatever", context);
        System.out.println(ir.ping());
    }

    public enum HOLDER implements RemoteContextType<String> {
        VALUE {
            @Override
            public Class<String> getValueType() {
                return String.class;
            }
        };
    }

    @Path("/out-of-band")
    public interface InboxResource {
        @POST
        @Path("ping")
        String ping();
    }

    public static class InboxResourceImpl implements InboxResource {
        Supplier<String> sup = RemoteContextHolder.INBOX.getProviderForKey(HOLDER.VALUE);

        @Override
        public String ping() {
            return sup.get();
        }
    }

    static class OutboxShippingInterceptor implements RequestInterceptor {
        ObjectMapper mapper = new ObjectMapper();
        @Override
        public void apply(RequestTemplate template) {
            Map<RemoteContextType<?>, Object> map = RemoteContextHolder.OUTBOX.getHolderContext().get();
            Map<String, Object> toSerialize = Maps.newHashMap();
            for (Entry<RemoteContextType<?>, Object> e : map.entrySet()) {
                Enum<?> key = (Enum<?>) e.getKey();
                String keyStr = key.getDeclaringClass().getName() + "." + key.name();
                toSerialize.put(keyStr, e.getValue());
            }
            try {
                String headerString = mapper.writeValueAsString(toSerialize);
                System.out.println(headerString);
                template.header("OUT_OF_BAND", headerString);
            } catch (JsonProcessingException e) {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    static class InboxPopulatingContainerRequestFilter implements ContainerRequestFilter {
        ObjectMapper mapper = new ObjectMapper();
        @Override
        public void filter(ContainerRequestContext requestContext) throws IOException {
            AbstractWritableServiceContext<Map<RemoteContextType<?>, Object>> holderContext = (AbstractWritableServiceContext<Map<RemoteContextType<?>, Object>>) RemoteContextHolder.INBOX.getHolderContext();
            if (!requestContext.getHeaders().containsKey("OUT_OF_BAND")) {
                return;
            }
            String header = requestContext.getHeaders().get("OUT_OF_BAND").iterator().next();
            @SuppressWarnings("unchecked")
            Map<String, Object> map = mapper.readValue(header, Map.class);

            System.out.println(requestContext);
            System.out.println(map);
        }

    }

}
