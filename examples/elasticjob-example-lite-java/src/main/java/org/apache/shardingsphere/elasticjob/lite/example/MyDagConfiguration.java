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

package org.apache.shardingsphere.elasticjob.lite.example;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.dag.job.DagConfiguration;
import org.apache.shardingsphere.elasticjob.dag.job.DagJobConfigurer;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.simple.job.SimpleJob;


public final class MyDagConfiguration implements DagConfiguration {

    private final CoordinatorRegistryCenter registryCenter;

    public MyDagConfiguration(final CoordinatorRegistryCenter registryCenter) {
        this.registryCenter = registryCenter;
    }

    @Override
    public void configure(final DagJobConfigurer configurer) {
        configurer
                .name("dag-test")

                .newJob()
                .registryCenter(registryCenter)
                .elasticJob(new DagSimpleJob())
                .jobConfiguration(JobConfiguration.newBuilder("root-0", 3).build())
                .end()

                .newJob()
                .registryCenter(registryCenter)
                .elasticJob(new DagSimpleJob())
                .jobConfiguration(JobConfiguration.newBuilder("job-0", 6).build())
                .parent("root-0")
                .end()

                .newJob()
                .registryCenter(registryCenter)
                .elasticJob(new DagSimpleJob())
                .jobConfiguration(JobConfiguration.newBuilder("job-1", 2).build())
                .parent("job-0")
                .end()

                .newJob()
                .registryCenter(registryCenter)
                .elasticJob(new DagSimpleJob())
                .jobConfiguration(JobConfiguration.newBuilder("root-1", 3).build())
                .end()

                .newJob()
                .registryCenter(registryCenter)
                .elasticJob(new DagSimpleJob())
                .jobConfiguration(JobConfiguration.newBuilder("last", 2).build())
                .parent("job-1")
                .parent("root-1")
                .end();
    }

    @Slf4j
    private static final class DagSimpleJob implements SimpleJob {

        @SneakyThrows
        @Override
        public void execute(final ShardingContext shardingContext) {
            log.info("{} [{}] started", shardingContext.getJobName(), shardingContext.getShardingItem());
            long took = (long) (Math.random() * 3000);
            Thread.sleep(took);
            log.info("{} [{}] done (took {})", shardingContext.getJobName(), shardingContext.getShardingItem(), took);
        }
    }
}
