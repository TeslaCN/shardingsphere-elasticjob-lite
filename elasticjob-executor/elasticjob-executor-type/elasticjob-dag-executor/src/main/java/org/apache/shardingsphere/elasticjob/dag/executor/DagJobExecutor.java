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

package org.apache.shardingsphere.elasticjob.dag.executor;

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.dag.Dag;
import org.apache.shardingsphere.elasticjob.dag.Node;
import org.apache.shardingsphere.elasticjob.dag.internal.BlockingOneOffJobBootstrap;
import org.apache.shardingsphere.elasticjob.dag.internal.SimpleDag;
import org.apache.shardingsphere.elasticjob.dag.job.DagConfiguration;
import org.apache.shardingsphere.elasticjob.dag.job.DagJobConfigurer;
import org.apache.shardingsphere.elasticjob.executor.JobFacade;
import org.apache.shardingsphere.elasticjob.executor.item.impl.ClassedJobItemExecutor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public final class DagJobExecutor implements ClassedJobItemExecutor<DagConfiguration> {
    
    /**
     * TODO Make this class stateless.
     */
    private final ConcurrentMap<String, Dag> configured = new ConcurrentHashMap<>();
    
    @Override
    public void process(final DagConfiguration dagConfiguration, final JobConfiguration jobConfig, final JobFacade jobFacade, final ShardingContext shardingContext) {
        // TODO Each job should be configured only once on each node.
        log.info("[{}-{}] started", shardingContext.getJobName(), shardingContext.getShardingItem());
        configured.computeIfAbsent(jobConfig.getJobName(), unused -> configureDag(dagConfiguration));
        if (!isMaster(shardingContext.getShardingItem())) {
            return;
        }
        configured.get(jobConfig.getJobName()).run();
    }
    
    private Dag configureDag(final DagConfiguration dagConfiguration) {
        final DagJobConfigurer dagJobConfigurer = new DagJobConfigurer();
        dagConfiguration.configure(dagJobConfigurer);
        List<DagJobConfigurer.DagJobConfiguration> configurations = dagJobConfigurer.getDagJobConfigurations();
        Dag dag = new SimpleDag(dagJobConfigurer.getName());
        configurations.stream().map(config -> {
            String nodeId = config.getJobConfiguration().getJobName();
            final Set<String> parents = config.getParents();
            final AtomicReference<BlockingOneOffJobBootstrap> bootstrapReference = new AtomicReference<>();
            bootstrapReference.set(new BlockingOneOffJobBootstrap(config.getRegistryCenter(), config.getElasticJob(), config.getJobConfiguration(), config.getTracing()));
            return new Node(nodeId, () -> {
                try {
                    bootstrapReference.get().run();
                } catch (final InterruptedException ex) {
                    ex.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }, parents);
        }).forEach(dag::addNodes);
        return dag;
    }
    
    private boolean isMaster(final int shardingItem) {
        return 0 == shardingItem;
    }
    
    @Override
    public Class<DagConfiguration> getElasticJobClass() {
        return DagConfiguration.class;
    }
}
