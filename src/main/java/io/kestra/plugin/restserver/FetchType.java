package io.kestra.plugin.restserver;

/**
 * What the trigger does with the body of an incoming request.
 * <p>
 * File parts of a {@code multipart/form-data} request are not governed by this: they are always stored, because a
 * file has no useful representation inside an execution. This decides the fate of the request body only.
 */
public enum FetchType {

    /**
     * Read the body off the connection and drop it. The body is read rather than left unclaimed so that the caller
     * gets its response over a connection that was not cut short mid-upload; no byte of it is kept.
     */
    NONE,

    /**
     * Expose the body to the flow as {@code trigger.body}, decoded as a string. With {@code base64Body} it is also
     * exposed as {@code trigger.bodyBase64}. This is the default, and what the trigger has always done.
     */
    FETCH,

    /**
     * Stream the body into Kestra's internal storage as it is received, and expose its URI as {@code trigger.uri}.
     * No part of it is held in memory, and none of it travels through the execution record.
     */
    STORE
}
