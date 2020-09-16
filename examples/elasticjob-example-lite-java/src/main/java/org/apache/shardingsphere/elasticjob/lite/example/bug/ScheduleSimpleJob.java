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

package org.apache.shardingsphere.elasticjob.lite.example.bug;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.lite.api.bootstrap.impl.OneOffJobBootstrap;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.simple.job.SimpleJob;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@RequiredArgsConstructor
public final class ScheduleSimpleJob implements SimpleJob {
    
    private static final ConcurrentMap<String, OneOffJobBootstrap> JOBS = new ConcurrentHashMap<>();
    
    private final String scheduleJobName;
    
    private final CoordinatorRegistryCenter registryCenter;
    
    @Override
    public void execute(final ShardingContext shardingContext) {
        JOBS.computeIfAbsent(scheduleJobName, unused -> {
            JobConfiguration jobConfiguration = JobConfiguration.newBuilder(scheduleJobName, 3).build();
            return new OneOffJobBootstrap(registryCenter, new SomeSimpleJob(), jobConfiguration);
        });
        if (0 != shardingContext.getShardingItem()) {
            return;
        }
        log.info("Execute job {} on 0", scheduleJobName);
        executeOneOffJob();
    }
    
    private void executeOneOffJob() {
        JOBS.get(scheduleJobName).execute();
    }
    
    @Slf4j
    public static final class SomeSimpleJob implements SimpleJob {
        
        @SneakyThrows
        @Override
        public void execute(final ShardingContext shardingContext) {
            long since = System.currentTimeMillis();
            log.info("{} [{}] started.", shardingContext.getJobName(), shardingContext.getShardingItem());
            double took = Math.random() * 5000;
            TimeUnit.MILLISECONDS.sleep((long) took);
            log.info("{} [{}] finished. Took {}", shardingContext.getJobName(), shardingContext.getShardingItem(), System.currentTimeMillis() - since);
        }
    }
}
