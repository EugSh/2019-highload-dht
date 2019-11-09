package ru.mail.polis.service.shkalev;

import one.nio.http.Request;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.dao.shkalev.Row;

import java.net.http.HttpResponse;

final class ServiceUtils {
    static final String PROXY_HEADER = "Is-Proxy";
    static final String VALUE_PROXY_HEADER = "True";
    private static final String TIME_HEADER = "timestamp";
    private static final String SEP_HEADER = ":";
    private static final int CREATED = 201;
    private static final int ACCEPTED = 202;
    private static final int NOT_REPLICAS = 504;
    private static final int OK = 200;
    private static final int NOT_FOUND = 404;
    private static final int BAD_REQUEST = 400;
    private static final int INTERNAL_ERROR = 500;
    static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

    private ServiceUtils() {
    }

    static boolean isProxied(@NotNull final Request request) {
        return request.getHeader(PROXY_HEADER) != null;
    }

    static long getTimeStamp(@NotNull final Response response) {
        final String timeHeader = response.getHeader(TIME_HEADER + SEP_HEADER);
        return timeHeader == null ? -1 : Long.parseLong(timeHeader);
    }

    static long getTimeStamp(@NotNull final HttpResponse<byte[]> response) {
        return Long.parseLong(response.headers().firstValue(TIME_HEADER).orElse("-1"));
    }

    static Response responseFromRow(@NotNull final Row row) {
        if (row.isDead()) {
            final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
            response.addHeader(TIME_HEADER + SEP_HEADER + row.getTime());
            return response;
        }
        final byte[] body = new byte[row.getValue().remaining()];
        row.getValue().get(body);
        final Response response = new Response(Response.OK, body);
        response.addHeader(TIME_HEADER + SEP_HEADER + row.getTime());
        return response;
    }

    static String statusOf(final int status) {
        switch (status) {
            case CREATED:
                return Response.CREATED;
            case ACCEPTED:
                return Response.ACCEPTED;
            case OK:
                return Response.OK;
            case NOT_FOUND:
                return Response.NOT_FOUND;
            case NOT_REPLICAS:
                return NOT_ENOUGH_REPLICAS;
            case BAD_REQUEST:
                return Response.BAD_REQUEST;
            case INTERNAL_ERROR:
                return Response.INTERNAL_ERROR;
            default:
                throw new UnsupportedOperationException("Unsupported code status - " + status);
        }
    }

    static Response parse(@NotNull final HttpResponse<byte[]> response) {
        final Response result = new Response(statusOf(response.statusCode()), response.body());
        result.addHeader(TIME_HEADER + SEP_HEADER + getTimeStamp(response));
        return result;
    }
}
