/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.remoting;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.palantir.atlasdb.http.TextDelegateDecoder;
import com.palantir.atlasdb.keyvalue.remoting.outofband.InboxPopulatingContainerRequestFilter;
import com.palantir.atlasdb.keyvalue.remoting.outofband.OutboxShippingInterceptor;
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
            new InboxPopulatingContainerRequestFilter(new ObjectMapper()));

    @Test
    public void testSerializing() {
        ObjectMapper mapper = new ObjectMapper();
        String uri = dropwizard.baseUri().toString();
        InboxResource ir = Feign.builder()
                .decoder(new TextDelegateDecoder(new JacksonDecoder()))
                .encoder(new JacksonEncoder(mapper))
                .contract(new JAXRSContract())
                .requestInterceptor(new OutboxShippingInterceptor(mapper))
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
