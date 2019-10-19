package ru.mail.polis.service.shkalev;

import com.google.common.base.Charsets;
import one.nio.http.Path;
import one.nio.http.Response;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import one.nio.http.Param;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Executor;

public class MyAsyncService extends HttpServer implements Service {
    private static final Logger log = LoggerFactory.getLogger(MyAsyncService.class);
    private final DAO dao;
    private final Executor executor;

    /**
     * Async Http Rest Service.
     *
     * @param port     port for HttpServer
     * @param dao      LSMDao
     * @param executor executor for async working
     * @throws IOException in init server
     */
    public MyAsyncService(final int port, @NotNull final DAO dao, @NotNull final Executor executor) throws IOException {
        super(getConfig(port));
        this.dao = dao;
        this.executor = executor;
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
     * Main resource for httpServer.
     *
     * @param request The request object in which the information is stored:
     *                the type of request (PUT, GET, DELETE) and the request body.
     * @param session HttpSession
     * @param id      Record ID is equivalent to the key in dao.
     * @throws IOException where send in session.
     */
    @Path("/v0/entity")
    public void entity(@NotNull final Request request,
                       @NotNull final HttpSession session,
                       @Param("id") final String id) throws IOException {
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No Id");
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeAsync(session, () -> get(key));
                break;
            case Request.METHOD_PUT:
                executeAsync(session, () -> put(request, key));
                break;
            case Request.METHOD_DELETE:
                executeAsync(session, () -> delete(key));
                break;
            default:
                session.sendError(Response.METHOD_NOT_ALLOWED, "Method not allowed");
                break;
        }

    }

    /**
     * Resource for getting status.
     *
     * @param request The request object in which the information is stored:
     *                the type of request (PUT, GET, DELETE) and the request body.
     * @param session HttpSession
     * @throws IOException where send in session.
     */
    @Path("/v0/status")
    public void entity(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.OK, Response.EMPTY));
    }

    @Override
    public HttpSession createSession(@NotNull final Socket socket) throws RejectedSessionException {
        return new StorageSession(socket, this);
    }

    /**
     * resourse for range values
     *
     * @param request The request object in which the information is stored:
     *                the type of request (PUT, GET, DELETE) and the request body.
     * @param session HttpSession.
     * @param start   start key for range.
     * @param end     end key for range.
     * @throws IOException where send in session.
     */
    @Path("/v0/entities")
    public void entities(@NotNull final Request request,
                         @NotNull final HttpSession session,
                         @Param("start") final String start,
                         @Param("end") final String end) throws IOException {
        if (start == null || start.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No start");
        }
        if (end != null && end.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "end is empty");
        }
        final ByteBuffer from = ByteBuffer.wrap(start.getBytes(Charsets.UTF_8));
        final ByteBuffer to = end == null ? null : ByteBuffer.wrap(end.getBytes(Charsets.UTF_8));
        try {
            final Iterator<Record> records = dao.range(from, to);
            ((StorageSession) session).stream(records);
        } catch (IOException e) {
            session.sendError(Response.INTERNAL_ERROR, "");
            log.error("IOException on entities", e);
        }
    }

    @Override
    public void handleDefault(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
    }

    private Response get(@NotNull final ByteBuffer key) {
        final ByteBuffer value;
        try {
            value = dao.get(key).duplicate();
            final byte[] body = new byte[value.remaining()];
            value.get(body);
            return new Response(Response.OK, body);
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response put(@NotNull final Request request,
                         @NotNull final ByteBuffer key) {
        try {
            dao.upsert(key, ByteBuffer.wrap(request.getBody()));
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final ByteBuffer key) {
        try {
            dao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (IOException e) {
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private void executeAsync(@NotNull final HttpSession session,
                              @NotNull final Action action) {
        executor.execute(() -> {
            try {
                session.sendResponse(action.action());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error("IOException on session send error", e);
                }
            }
        });
    }
}
