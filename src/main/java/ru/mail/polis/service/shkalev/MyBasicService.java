package ru.mail.polis.service.shkalev;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import com.google.common.base.Charsets;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.server.AcceptorConfig;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

public class MyBasicService extends HttpServer implements Service {

    private final DAO dao;

    public MyBasicService(final int port, @NotNull final DAO dao) throws IOException {
        super(getConfig(port));
        this.dao = dao;
    }

    private static HttpServerConfig getConfig(final int port) {
        if (port <= 1024 || port >= 65535) {
            throw new IllegalArgumentException();
        }
        final AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        final HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{acceptor};
        return config;
    }

    /**
     * Main resource for access
     *
     * @param request The request object in which the information is stored:
     *                the type of request (PUT, GET, DELETE) and the request body
     * @param id Record ID is equivalent to the key in dao
     * @return returns a response depending on the type of request and id
     */
    @Path("/v0/entity")
    public Response entity(@NotNull final Request request,
                           @Param("id") final String id) {
        if (id == null || id.isEmpty()) {
            return new Response(Response.BAD_REQUEST, Response.EMPTY);
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        Response response;
        try {
            response = getResponse(request, key);
        } catch (NoSuchElementException e) {
            response = new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            response = new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
        return response;
    }

    @Path("/v0/status")
    public Response entity(@NotNull final Request request) {
        return new Response(Response.OK, Response.EMPTY);
    }

    private Response getResponse(@NotNull final Request request, @NotNull final ByteBuffer key) throws IOException {
        Response response;
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                final ByteBuffer value = dao.get(key).duplicate();
                final byte[] body = new byte[value.remaining()];
                value.get(body);
                response = new Response(Response.OK, body);
                break;
            case Request.METHOD_PUT:
                dao.upsert(key, ByteBuffer.wrap(request.getBody()));
                response = new Response(Response.CREATED, Response.EMPTY);
                break;
            case Request.METHOD_DELETE:
                dao.remove(key);
                response = new Response(Response.ACCEPTED, Response.EMPTY);
                break;
            default:
                response = new Response(Response.METHOD_NOT_ALLOWED, Response.EMPTY);
                break;
        }
        return response;
    }

    @Override
    public void handleDefault(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        final Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }
}
