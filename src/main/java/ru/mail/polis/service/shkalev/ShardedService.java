package ru.mail.polis.service.shkalev;

import com.google.common.base.Charsets;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.http.HttpClient;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.net.ConnectionString;
import one.nio.net.Socket;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.shkalev.AdvancedDAO;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

public class ShardedService<T> extends HttpServer implements Service {
    private static final Logger log = LoggerFactory.getLogger(ShardedService.class);
    private final AdvancedDAO dao;
    private final Replicas quorum;
    private final Replicator<T> replicator;

    /**
     * Async sharded Http Rest Service.
     *
     * @param port     port for HttpServer
     * @param dao      LSMDao
     * @param executor executor for async working
     * @throws IOException in init server
     */
    public ShardedService(final int port,
                          @NotNull final DAO dao,
                          @NotNull final Executor executor,
                          @NotNull final Topology<T> nodes) throws IOException {
        super(getConfig(port));
        this.dao = (AdvancedDAO) dao;
        Map<T, HttpClient> pool = new HashMap<>();
        this.quorum = Replicas.quorum(nodes.size());
        for (final T node : nodes.all()) {
            if (nodes.isMe(node)) {
                continue;
            }
            assert !pool.containsKey(node);
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
        replicator = new Replicator<T>(nodes, executor, pool, this.dao);
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
     * @param request  The request object in which the information is stored:
     *                 the type of request (PUT, GET, DELETE) and the request body.
     * @param session  HttpSession
     * @param id       Record ID is equivalent to the key in dao.
     * @param replicas Кeplication factor.
     * @throws IOException where send in session.
     */
    @Path("/v0/entity")
    public void entity(@NotNull final Request request,
                       @NotNull final HttpSession session,
                       @Param("id") final String id,
                       @Param("replicas") final String replicas) throws IOException {
        if (id == null || id.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "No Id");
            return;
        }
        final boolean isProxy = ServiceUtils.isProxied(request);
        final Replicas rf = isProxy || replicas == null ? quorum : Replicas.parse(replicas);
        if (rf.getAck() > rf.getFrom() || rf.getAck() <= 0) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                replicator.executeGet(session, request, key, isProxy, rf);
                break;
            case Request.METHOD_PUT:
                replicator.executePut(session, request, key, isProxy, rf);
                break;
            case Request.METHOD_DELETE:
                replicator.executeDelete(session, request, key, isProxy, rf);
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
     * Resource for range values.
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
            return;
        }
        if (end != null && end.isEmpty()) {
            session.sendError(Response.BAD_REQUEST, "end is empty");
            return;
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
}
