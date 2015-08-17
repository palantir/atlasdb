package com.palantir.server;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.client.TextDelegateDecoder;
import com.palantir.common.supplier.PopulateServiceContextProxy;
import com.palantir.common.supplier.RemoteContextHolder;
import com.palantir.common.supplier.RemoteContextHolder.RemoteContextType;
import com.palantir.common.supplier.ServiceContext;

import feign.Feign;
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
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .requestInterceptor(new OutboxShippingInterceptor())
                .target(InboxResource.class, uri);
        ServiceContext<String> context = RemoteContextHolder.OUTBOX.getProviderForKey(HOLDER.VALUE);
        ir = PopulateServiceContextProxy.newProxyInstanceWithConstantValue(InboxResource.class, ir, "whatever", context);
        Assert.assertEquals("whatever", ir.ping());
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
        @Produces(MediaType.TEXT_PLAIN)
        String ping();
    }

    public static class InboxResourceImpl implements InboxResource {
        Supplier<String> sup = RemoteContextHolder.INBOX.getProviderForKey(HOLDER.VALUE);

        @Override
        public String ping() {
            return sup.get();
        }
    }

}
