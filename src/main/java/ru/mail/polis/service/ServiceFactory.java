/*
 * Copyright 2019 (c) Odnoklassniki
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.mail.polis.service;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jetbrains.annotations.NotNull;

import ru.mail.polis.dao.DAO;
import ru.mail.polis.service.shkalev.Address;
import ru.mail.polis.service.shkalev.Ring;
import ru.mail.polis.service.shkalev.ShardedService;
import ru.mail.polis.service.shkalev.Topology;

/**
 * Constructs {@link Service} instances.
 *
 * @author Vadim Tsesko
 */
public final class ServiceFactory {
    private static final long MAX_HEAP = 256 * 1024 * 1024;

    private ServiceFactory() {
        // Not supposed to be instantiated
    }

    /**
     * Construct a storage instance.
     *
     * @param port port to bind HTTP server to
     * @param dao  DAO to store the data
     * @return a storage instance
     */
    @NotNull
    public static Service create(
            final int port,
            @NotNull final DAO dao,
            final Set<String> topology) throws IOException {
        if (Runtime.getRuntime().maxMemory() > MAX_HEAP) {
            throw new IllegalStateException("The heap is too big. Consider setting Xmx.");
        }

        if (port <= 0 || 65536 <= port) {
            throw new IllegalArgumentException("Port out of range");
        }

        final Topology<Address> ring = new Ring(topology,
                "http://localhost:" + port,
                3);
        final Executor executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(),
                new ThreadFactoryBuilder().setNameFormat("asyncWorker").build());
        return new ShardedService(port, dao, executor, ring);
    }
}
