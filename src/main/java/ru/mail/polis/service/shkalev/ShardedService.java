package ru.mail.polis.service.shkalev;

import com.google.common.base.Charsets;
import one.nio.http.HttpException;
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
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import one.nio.server.RejectedSessionException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.shkalev.AdvancedDAO;
import ru.mail.polis.dao.shkalev.Row;
import ru.mail.polis.service.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShardedService<T> extends HttpServer implements Service {
    private static final Logger log = LoggerFactory.getLogger(ShardedService.class);
    private final AdvancedDAO dao;
    private final Executor executor;
    private final Topology<T> topology;
    private final Map<T, HttpClient> pool;
    private final Replicas quorum;
    private static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";

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
        this.executor = executor;
        this.topology = nodes;
        this.pool = new HashMap<>();
        this.quorum = Replicas.quorum(nodes.size());
        for (final T node : nodes.all()) {
            if (topology.isMe(node)) {
                continue;
            }
            assert !pool.containsKey(node);
            pool.put(node, new HttpClient(new ConnectionString(node + "?timeout=100")));
        }
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
        final Replicas r = isProxy || replicas == null ? quorum : Replicas.parse(replicas);
        if (r.getAck() > r.getFrom() || r.getAck() <= 0) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        final ByteBuffer key = ByteBuffer.wrap(id.getBytes(Charsets.UTF_8));
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                executeGet(session, request, key, isProxy, r);
                break;
            case Request.METHOD_PUT:
                executePut(session, request, key, isProxy, r);
                break;
            case Request.METHOD_DELETE:
                executeDelete(session, request, key, isProxy, r);
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

    private Response get(@NotNull final ByteBuffer key) {
        final Row row;
        try {
            row = dao.getRow(key);
            if (row.isDead()) {
                final Response response = new Response(Response.NOT_FOUND, Response.EMPTY);
                response.addHeader(ServiceUtils.TIME_HEADER + row.getTime());
                return response;
            }
            final byte[] body = new byte[row.getValue().remaining()];
            row.getValue().get(body);
            final Response response = new Response(Response.OK, body);
            response.addHeader(ServiceUtils.TIME_HEADER + row.getTime());
            return response;
        } catch (NoSuchElementException e) {
            return new Response(Response.NOT_FOUND, Response.EMPTY);
        } catch (IOException e) {
            log.error("Cant get from dao", e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response put(@NotNull final Request request,
                         @NotNull final ByteBuffer key) {
        try {
            dao.upsert(key, ByteBuffer.wrap(request.getBody()));
            return new Response(Response.CREATED, Response.EMPTY);
        } catch (IOException e) {
            log.error("Cant upsert into dao", e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response delete(@NotNull final ByteBuffer key) {
        try {
            dao.remove(key);
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } catch (IOException e) {
            log.error("Cant remove from dao", e);
            return new Response(Response.INTERNAL_ERROR, Response.EMPTY);
        }
    }

    private Response proxy(@NotNull final T node, @NotNull final Request request) {
        assert !topology.isMe(node);
        try {
            return pool.get(node).invoke(request);
        } catch (InterruptedException | PoolException | HttpException | IOException e) {
            log.error("Cant proxy", e);
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

    private void executeGet(@NotNull final HttpSession session,
                            @NotNull final Request request,
                            @NotNull final ByteBuffer key,
                            final boolean isProxy,
                            @NotNull final Replicas replicas) {
        if (isProxy) {
            executeAsync(session, () -> get(key));
            return;
        }
        executor.execute(() -> {
            final List<Response> result = executeReplication(() -> get(key),
                    request,
                    key,
                    replicas).stream().filter(ServiceUtils::validResponse).collect(Collectors.toList());
            try {
                if (result.size() < replicas.getAck()) {

                    session.sendResponse(new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY));
                    return;
                }
                Comparator<Map.Entry<Response, Integer>> comparator = Comparator.comparingInt(e -> e.getValue());
                final Map.Entry<Response, Integer> codesCount = result.stream()
                        .collect(Collectors.toMap(Function.identity(),
                                r -> 1,
                                Integer::sum))
                        .entrySet()
                        .stream()
                        .max(comparator.thenComparingLong(e -> ServiceUtils.getTimeStamp(e.getKey())))
                        .get();
                session.sendResponse(codesCount.getKey());
            } catch (IOException e) {
                try {
                    session.sendError(Response.INTERNAL_ERROR, "");
                } catch (IOException ex) {
                    log.error("IOException on session send error", e);
                }
            }
        });
    }

    private List<Response> executeReplication(@NotNull final Action localAction,
                                              @NotNull final Request request,
                                              @NotNull final ByteBuffer key,
                                              @NotNull final Replicas replicas) {
        request.addHeader(ServiceUtils.PROXY_HEADER);
        final Set<T> nodes = topology.primaryFor(key, replicas);
        final List<Response> result = new ArrayList<>(nodes.size());
        for (T node : nodes) {
            if (topology.isMe(node)) {
                result.add(localAction.action());
            } else {
                result.add(proxy(node, request));
            }
        }
        return result;
    }

    private void executePut(@NotNull final HttpSession session,
                            @NotNull final Request request,
                            @NotNull final ByteBuffer key,
                            final boolean isProxy,
                            @NotNull final Replicas replicas) {
        if (isProxy) {
            executeAsync(session, () -> put(request, key));
            return;
        }
        executor.execute(() -> {
            final List<Response> result = executeReplication(() -> put(request, key),
                    request,
                    key,
                    replicas);
            long countPutKeys = result.stream()
                    .filter(node -> node.getHeaders()[0].equals(Response.CREATED))
                    .count();
            acceptReplicas(countPutKeys,
                    session,
                    new Response(Response.CREATED, Response.EMPTY),
                    new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY),
                    replicas);
        });
    }

    private void acceptReplicas(final long countAck,
                                @NotNull final HttpSession session,
                                @NotNull final Response successfulResponse,
                                @NotNull final Response faildReplicas,
                                @NotNull final Replicas replicas) {
        try {
            if (countAck >= replicas.getAck()) {
                session.sendResponse(successfulResponse);

            } else {
                session.sendResponse(faildReplicas);
            }
        } catch (IOException e) {
            try {
                session.sendError(Response.INTERNAL_ERROR, "");
            } catch (IOException ex) {
                log.error("IOException on session send error", e);
            }
        }
    }

    private void executeDelete(@NotNull final HttpSession session,
                               @NotNull final Request request,
                               @NotNull final ByteBuffer key,
                               final boolean isProxy,
                               @NotNull final Replicas replicas) {
        if (isProxy) {
            executeAsync(session, () -> delete(key));
            return;
        }
        executor.execute(() -> {
            List<Response> result = executeReplication(() -> delete(key),
                    request,
                    key,
                    replicas);
            final long countDeleted = result.stream().filter(node -> node.getHeaders()[0].equals(Response.ACCEPTED)).count();
            acceptReplicas(countDeleted,
                    session,
                    new Response(Response.ACCEPTED, Response.EMPTY),
                    new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY),
                    replicas);
        });
    }
}
