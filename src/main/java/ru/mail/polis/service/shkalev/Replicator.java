package ru.mail.polis.service.shkalev;


import one.nio.http.HttpSession;
import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.shkalev.AdvancedDAO;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

class Replicator<T> {
    private static final String NOT_ENOUGH_REPLICAS = "504 Not Enough Replicas";
    private static final String IO_EXCEPTION_MSG = "IOException on session send error";
    private final Logger log = LoggerFactory.getLogger(Replicator.class);
    private final Executor executor;
    private final Topology<T> topology;
    private final Map<T, HttpClient> pool;
    private final AdvancedDAO dao;

    Replicator(@NotNull final Topology<T> topology, @NotNull final Executor executor,
               @NotNull final Map<T, HttpClient> pool, @NotNull final AdvancedDAO dao) {
        this.executor = executor;
        this.topology = topology;
        this.pool = pool;
        this.dao = dao;
    }

    private Response get(@NotNull final ByteBuffer key) {
        try {
            return ServiceUtils.responseFromRow(dao.getRow(key));
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

    private Response proxy(@NotNull final T node, @NotNull final Request rf) {
        assert !topology.isMe(node);
        try {
            return pool.get(node).invoke(rf);
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
                sendError(session, e);
            }
        });
    }

    void executeGet(@NotNull final HttpSession session, @NotNull final Request request,
                    @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf) {
        if (isProxy) {
            executeAsync(session, () -> get(key));
            return;
        }
        executor.execute(() -> {
            final List<Response> result = executeReplication(() -> get(key), request, key, rf)
                    .stream().filter(ServiceUtils::validResponse).collect(Collectors.toList());
            try {
                if (result.size() < rf.getAck()) {
                    session.sendResponse(new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY));
                    return;
                }
                final Comparator<Map.Entry<Response, Integer>> comparator = Comparator.comparingInt(e -> e.getValue());
                final Map.Entry<Response, Integer> codesCount = result.stream()
                        .collect(Collectors.toMap(Function.identity(), r -> 1, Integer::sum)).entrySet().stream()
                        .max(comparator.thenComparingLong(e -> ServiceUtils.getTimeStamp(e.getKey()))).get();
                session.sendResponse(codesCount.getKey());
            } catch (IOException e) {
                sendError(session, e);
            }
        });
    }

    private List<Response> executeReplication(@NotNull final Action localAction, @NotNull final Request request,
                                              @NotNull final ByteBuffer key, @NotNull final Replicas rf) {
        request.addHeader(ServiceUtils.PROXY_HEADER);
        final Set<T> nodes = topology.primaryFor(key, rf);
        final List<Response> result = new ArrayList<>(nodes.size());
        for (final T node : nodes) {
            if (topology.isMe(node)) {
                result.add(localAction.action());
            } else {
                result.add(proxy(node, request));
            }
        }
        return result;
    }

    void executePut(@NotNull final HttpSession session, @NotNull final Request request,
                    @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf) {
        if (isProxy) {
            executeAsync(session, () -> put(request, key));
            return;
        }
        executor.execute(() -> {
            final List<Response> result = executeReplication(() -> put(request, key), request, key, rf);
            final long countPutKeys = result.stream().filter(node -> node.getHeaders()[0].equals(Response.CREATED))
                    .count();
            acceptReplicas(countPutKeys, session, new Response(Response.CREATED, Response.EMPTY),
                    new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY), rf);
        });
    }

    private void acceptReplicas(final long countAck, @NotNull final HttpSession session,
                                @NotNull final Response successfulResponse, @NotNull final Response faildReplicas,
                                @NotNull final Replicas rf) {
        try {
            if (countAck >= rf.getAck()) {
                session.sendResponse(successfulResponse);

            } else {
                session.sendResponse(faildReplicas);
            }
        } catch (IOException e) {
            sendError(session, e);
        }
    }

    void executeDelete(@NotNull final HttpSession session, @NotNull final Request request,
                       @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf) {
        if (isProxy) {
            executeAsync(session, () -> delete(key));
            return;
        }
        executor.execute(() -> {
            final List<Response> result = executeReplication(() -> delete(key), request, key, rf);
            final long countDeleted = result.stream().filter(node -> node.getHeaders()[0].equals(Response.ACCEPTED))
                    .count();
            acceptReplicas(countDeleted, session, new Response(Response.ACCEPTED, Response.EMPTY),
                    new Response(NOT_ENOUGH_REPLICAS, Response.EMPTY), rf);
        });
    }

    private void sendError(@NotNull final HttpSession session, @NotNull final Exception e) {
        try {
            session.sendError(Response.INTERNAL_ERROR, "");
        } catch (IOException ex) {
            log.error(IO_EXCEPTION_MSG, e);
        }
    }
}
