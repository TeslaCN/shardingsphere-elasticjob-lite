/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.elasticjob.dag.internal;

import org.apache.shardingsphere.elasticjob.dag.Dag;
import org.apache.shardingsphere.elasticjob.dag.DagInstance;
import org.apache.shardingsphere.elasticjob.dag.Node;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Simple implementation.
 */
public final class SimpleDagInstance implements DagInstance {
    
    private final Dag dag;
    
    private final ConcurrentMap<String, ConcurrentMap<String, Object>> dependencies = new ConcurrentHashMap<>();
    
    private final AtomicInteger seq = new AtomicInteger();
    
    private final ExecutorService executorService;
    
    private final BlockingQueue<Node> queue = new LinkedBlockingQueue<>();
    
    private final Object empty = new Object();
    
    private final List<CompletableFuture<Void>> futures = new LinkedList<>();
    
    public SimpleDagInstance(final Dag dag) {
        this.dag = dag;
        for (Node each : dag.getNodes()) {
            dependencies.put(each.id(), each.parents().stream().collect(Collectors.toConcurrentMap(k -> k, v -> empty)));
        }
        final ThreadFactory threadFactory = runnable -> new Thread(runnable, String.format("%s-%s", dag.id(), seq.getAndIncrement()));
        executorService = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 2,
                1, TimeUnit.MINUTES, new LinkedBlockingQueue<>(), threadFactory);
    }
    
    @Override
    public void run() {
        while (!(dependencies.isEmpty() && queue.isEmpty())) {
            enqueueRunnableNodes();
            Node node = null;
            try {
                node = queue.poll(10, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            if (null == node) {
                continue;
            }
            final String nodeId = node.id();
            final CompletableFuture<Void> future = CompletableFuture
                    .runAsync(node::run, executorService)
                    .whenCompleteAsync((unused, throwable) -> {
                        for (String key : dependencies.keySet()) {
                            dependencies.computeIfPresent(key, (s, parents) -> {
                                parents.remove(nodeId);
                                return parents;
                            });
                        }
                    }, executorService);
            futures.add(future);
        }
        joinUntilAllDone();
        executorService.shutdown();
    }
    
    private void enqueueRunnableNodes() {
        final Iterator<Map.Entry<String, ConcurrentMap<String, Object>>> iterator = dependencies.entrySet().iterator();
        while (iterator.hasNext()) {
            final Map.Entry<String, ConcurrentMap<String, Object>> entry = iterator.next();
            if (!entry.getValue().isEmpty()) {
                continue;
            }
            queue.add(dag.getNode(entry.getKey()));
            iterator.remove();
        }
    }
    
    private void joinUntilAllDone() {
        for (CompletableFuture<Void> each : futures) {
            if (!each.isDone()) {
                each.join();
            }
        }
    }
}
