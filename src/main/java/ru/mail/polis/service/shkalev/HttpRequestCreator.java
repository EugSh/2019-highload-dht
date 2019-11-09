package ru.mail.polis.service.shkalev;

import one.nio.http.Request;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class HttpRequestCreator {
    private final Replicas rf;
    private final String key;
    private final byte[] body;
    private final int codeStatus;
    private static final Duration TIME_OUT = Duration.ofMillis(500);

    HttpRequestCreator(@NotNull final Replicas rf, @NotNull final ByteBuffer key,
                       @NotNull final Request request, final int codeStatus) {
        this.key = StandardCharsets.UTF_8.decode(key).toString();
        this.rf = rf;
        this.body = request.getBody();
        this.codeStatus = codeStatus;

    }

    HttpRequest create(@NotNull final Address address) {
        switch (this.codeStatus) {
            case Request.METHOD_GET:
                return get(address);
            case Request.METHOD_PUT:
                return put(address);
            case Request.METHOD_DELETE:
                return delete(address);
            default:
                throw new UnsupportedOperationException("Unsupported code status");
        }
    }

    private HttpRequest get(@NotNull final Address address) {
        return defaultBuilder(address.toString())
                .GET()
                .build();
    }

    private HttpRequest put(@NotNull final Address address) {
        return defaultBuilder(address.toString())
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
    }

    private HttpRequest delete(@NotNull final Address address) {
        return defaultBuilder(address.toString())
                .DELETE()
                .build();
    }

    private HttpRequest.Builder defaultBuilder(@NotNull final String address) {
        return HttpRequest.newBuilder()
                .uri(URI.create(address + "/v0/entity?id=" + key + "&replicas=" + rf.toString()))
                .timeout(TIME_OUT)
                .headers(ServiceUtils.PROXY_HEADER, ServiceUtils.VALUE_PROXY_HEADER);
    }
}
