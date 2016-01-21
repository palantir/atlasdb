package com.palantir.paxos;

import java.util.Collection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/paxos/{logName}")
public interface PaxosManyLogApi {

    @POST
    @Path("learn/{seq:.+}")
    @Consumes(MediaType.APPLICATION_JSON)
    void learn(@PathParam("logName") String logName, @PathParam("seq") long seq, PaxosValue val);

    @Nullable
    @GET
    @Path("learned-value/{seq:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    PaxosValue getLearnedValue(@PathParam("logName") String logName, @PathParam("seq") long seq);

    @Nullable
    @GET
    @Path("greatest-learned-value")
    @Produces(MediaType.APPLICATION_JSON)
    PaxosValue getGreatestLearnedValue(@PathParam("logName") String logName);

    @Nonnull
    @GET
    @Path("learned-values-since/{seq:.+}")
    @Produces(MediaType.APPLICATION_JSON)
    Collection<PaxosValue> getLearnedValuesSince(@PathParam("logName") String logName, @PathParam("seq") long seq);

    @POST
    @Path("prepare/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    PaxosPromise prepare(@PathParam("logName") String logName, @PathParam("seq") long seq, PaxosProposalId pid);

    @POST
    @Path("accept/{seq}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    BooleanPaxosResponse accept(@PathParam("logName") String logName, @PathParam("seq") long seq, PaxosProposal proposal);

    @POST // This is marked as a POST because we cannot accept stale or cached results for this method.
    @Path("latest-sequence-prepared-or-accepted")
    @Produces(MediaType.APPLICATION_JSON)
    long getLatestSequencePreparedOrAccepted(@PathParam("logName") String logName);

}
