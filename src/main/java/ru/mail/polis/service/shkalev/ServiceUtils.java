package ru.mail.polis.service.shkalev;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;

final class ServiceUtils {
    static final String PROXY_HEADER = "Is-Proxy: True";
    static final String TIME_HEADER = "Timestamp: ";

    private ServiceUtils() {
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(PROXY_HEADER) != null;
    }

    static long getTimeStamp(@NotNull final Response response) {
        final String timeHeader = response.getHeader(TIME_HEADER);
        return timeHeader == null ? -1 : Long.parseLong(timeHeader);
    }

    static boolean validResponse(@NotNull final Response response) {
        return response.getHeaders()[0].equals(Response.NOT_FOUND) || response.getHeaders()[0].equals(Response.OK);
    }

}
