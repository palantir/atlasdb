/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.timelock.paxos;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

public class WebsocketMessage {

    private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new GuavaModule());

    private final String messageId;
    private final String methodName;
    private final String[] args;

    public WebsocketMessage(String messageId, String methodName, String[] args) {
        this.messageId = messageId;
        this.methodName = methodName;
        this.args = args;
    }

    public String messageId() {
        return messageId;
    }

    public String methodName() {
        return methodName;
    }

    public String[] args() {
        return args;
    }

    public static String request(String messageId, String methodName, String...args) {
        return new WebsocketMessage(messageId, methodName, args).toString();
    }

    public static WebsocketMessage parse(String message) {
        String[] parts = StringUtils.split(message, ' ');

        String messageId = parts[0];
        String methodName = parts[1];

        if (parts.length == 2) {
            return new WebsocketMessage(messageId, methodName, new String[0]);
        }

        String[] args = Arrays.copyOfRange(parts, 2, parts.length);
        return new WebsocketMessage(messageId, methodName, args);
    }

    public String argAt(int index) {
        return args[index];
    }

    public long longAt(int index) {
        return Long.valueOf(args[index]);
    }

    public <T> T objAt(int index, Class<T> clazz) {
        return mapUnchecked(args[index], clazz);
    }

    public <T> T objAt(int index, TypeReference<T> clazz) {
        return mapUnchecked(args[index], clazz);
    }

    public WebsocketMessage response(String...args) {
        return new WebsocketMessage(messageId, methodName, args);
    }

    @Override
    public String toString() {
        String result = String.format("%s %s", messageId, methodName);

        if (args.length == 0) {
            return result;
        }

        return result + " " + String.join(" ", args);
    }

    public static <T> T mapUnchecked(String json, TypeReference<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T mapUnchecked(String json, Class<T> clazz) {
        try {
            return MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String mapUnchecked(Object obj) {
        try {
            return MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
