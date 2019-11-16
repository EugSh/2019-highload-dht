package ru.mail.polis.service.shkalev;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import one.nio.http.HttpSession;
import one.nio.http.Request;
import one.nio.http.Response;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.dao.shkalev.AdvancedDAO;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.RejectedExecutionException;
import java.util.Set;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;

public class AsyncReplicator implements Replicator {
    private static final String IO_EXCEPTION_MSG = "IOException on session send error";
    private final Logger log = LoggerFactory.getLogger(AsyncReplicator.class);
    private final Topology<Address> topology;
    private final AdvancedDAO dao;
    private final HttpClient client;
    private final Executor executor;

    /**
     * Class for replication of request to other nodes.
     *
     * @param executor executor for async working.
     * @param topology topology of cluster.
     * @param dao      LSMDao.
     */
    public AsyncReplicator(@NotNull final Executor executor,
                           @NotNull final Topology<Address> topology,
                           @NotNull final AdvancedDAO dao) {
        this.topology = topology;
        this.dao = dao;
        this.executor = executor;
        final Executor clientExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("asyncHttpClient").build());
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(1))
                .executor(clientExecutor)
                .build();
    }

    @Override
    public void executeGet(@NotNull final HttpSession session, @NotNull final Request request,
                           @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf) {
        if (isProxy) {
            execGet(session, key);
            return;
        }
        final Collection<CompletableFuture<Response>> futures = replication(() -> get(key),
                topology.primaryFor(key, rf),
                new HttpRequestCreator(rf, key.duplicate(), request, request.getMethod()));

        final CompletableFuture<Collection<Response>> future = collect(futures, rf.getAck());
        sendActualResponse(future, session);
    }

    private void execGet(@NotNull final HttpSession session,
                         @NotNull final ByteBuffer key) {
        executeAsync(session, () -> get(key));
    }

    @Override
    public void executePut(@NotNull final HttpSession session, @NotNull final Request request,
                           @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf) {
        if (isProxy) {
            executeAsync(session, () -> put(request, key));
            return;
        }
        final Collection<CompletableFuture<Response>> futures = replication(() -> put(request, key),
                topology.primaryFor(key, rf),
                new HttpRequestCreator(rf, key.duplicate(), request, request.getMethod()));
        final CompletableFuture<Collection<Response>> future = collect(futures, rf.getAck());
        sendActualResponse(future, session);
    }

    @Override
    public void executeDelete(@NotNull final HttpSession session, @NotNull final Request request,
                              @NotNull final ByteBuffer key, final boolean isProxy, @NotNull final Replicas rf) {
        if (isProxy) {
            executeAsync(session, () -> delete(key));
            return;
        }
        final Collection<CompletableFuture<Response>> futures = replication(() -> delete(key),
                topology.primaryFor(key, rf),
                new HttpRequestCreator(rf, key.duplicate(), request, request.getMethod()));
        final CompletableFuture<Collection<Response>> future = collect(futures, rf.getAck());
        sendActualResponse(future, session);
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

    private void sendError(@NotNull final HttpSession session, @NotNull final Exception e) {
        try {
            log.error(IO_EXCEPTION_MSG, e);
            session.sendError(Response.INTERNAL_ERROR, "");
        } catch (IOException ex) {
            log.error(IO_EXCEPTION_MSG, ex);
        }
    }

    private void sendResponse(@NotNull final HttpSession session, @NotNull final Response response) {
        try {
            session.sendResponse(response);
        } catch (IOException e) {
            sendError(session, e);
        }
    }

    private void sendActualResponse(@NotNull final CompletableFuture<Collection<Response>> future,
                                    @NonNull final HttpSession session) {
        future.whenCompleteAsync((responses, error) -> {
            if (error != null) {
                sendResponse(session, new Response(ServiceUtils.NOT_ENOUGH_REPLICAS, Response.EMPTY));
                return;
            }
            final Comparator<Map.Entry<Response, Integer>> comparator = Comparator.comparingInt(Map.Entry::getValue);
            final Optional<Map.Entry<Response, Integer>> actualResponse = responses.stream()
                    .collect(Collectors.toMap(Function.identity(), r -> 1, Integer::sum)).entrySet().stream()
                    .max(comparator.thenComparingLong(e -> ServiceUtils.getTimeStamp(e.getKey())));
            if (actualResponse.isPresent()) {
                sendResponse(session, actualResponse.get().getKey());
            } else {
                sendResponse(session, new Response(Response.INTERNAL_ERROR, Response.EMPTY));
            }
        }).exceptionally(e -> {
            log.error("Sending and merging futures - ", e);
            return null;
        });
    }

    private CompletableFuture<Collection<Response>> collect(@NotNull final Collection<CompletableFuture<Response>> f,
                                                            final int min) {
        final Collection<Response> res = new ArrayList<>(min);
        final Lock lock = new ReentrantLock();
        final AtomicInteger errors = new AtomicInteger(0);
        final int maxErrors = f.size() - min + 1;
        final CompletableFuture<Collection<Response>> future = new CompletableFuture<>();
        f.forEach(fu -> fu.whenCompleteAsync((r, e) -> {
            if (e != null) {
                if (errors.incrementAndGet() == maxErrors) {
                    future.completeExceptionally(new RejectedExecutionException(e));
                }
                return;
            }
            lock.lock();
            try{
                if (res.size() >= min){
                    return;
                }
                res.add(r);
                if (res.size() == min){
                    future.complete(res);
                }
            } finally {
                lock.unlock();
            }
        }).exceptionally(e -> {
            log.error("Collecting futures error - ", e);
            return null;
        }));
        return future;
    }

    private Collection<CompletableFuture<Response>> replication(@NotNull final Action localAction,
                                                                @NotNull final Set<Address> addresses,
                                                                @NotNull final HttpRequestCreator requestCreator) {
        final Collection<CompletableFuture<Response>> result = new ArrayList<>();
        for (final Address address : addresses) {
            if (topology.isMe(address)) {
                result.add(CompletableFuture.supplyAsync(localAction::action));
            } else {
                result.add(client.sendAsync(requestCreator.create(address),
                        HttpResponse.BodyHandlers.ofByteArray())
                        .thenApplyAsync(ServiceUtils::parse));
            }
        }
        return result;
    }
}
