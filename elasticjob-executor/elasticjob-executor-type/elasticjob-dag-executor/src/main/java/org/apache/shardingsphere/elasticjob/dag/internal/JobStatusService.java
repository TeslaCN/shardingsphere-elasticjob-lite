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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.listener.AbstractJobListener;
import org.apache.shardingsphere.elasticjob.lite.internal.sharding.ExecutionService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

@Slf4j
public final class JobStatusService {
    
    private final String jobName;
    
    private final CoordinatorRegistryCenter registryCenter;
    
    private final ExecutionService executionService;
    
    private final ConfigurationService configurationService;
    
    private final JobNodeStorage jobNodeStorage;
    
    public JobStatusService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        registryCenter = regCenter;
        executionService = new ExecutionService(regCenter, jobName);
        configurationService = new ConfigurationService(regCenter, jobName);
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
    }
    
    /**
     * Wait until all shards have finished.
     *
     * @throws InterruptedException interrupt
     */
    public void join() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(configurationService.load(false).getShardingTotalCount());
        final JobFinishListener listener = new JobFinishListener(countDownLatch);
        jobNodeStorage.addDataListener(listener);
        log.debug("Wait for {} finished", jobName);
        countDownLatch.await();
        log.debug("{} finished", jobName);
        // TODO Remove data listener
    }
    
    @RequiredArgsConstructor
    class JobFinishListener extends AbstractJobListener {
        
        private final JobNodePath jobNodePath = new JobNodePath(jobName);
        
        private final Pattern runningNodePattern = Pattern.compile(String.format("%s/\\d+/running", jobNodePath.getShardingNodePath()));
        
        private final CountDownLatch countDownLatch;
        
        @Override
        protected void dataChanged(final String path, final Type eventType, final String data) {
            if (Type.NODE_DELETED != eventType || !runningNodePattern.matcher(path).matches()) {
                return;
            }
            countDownLatch.countDown();
        }
    }
}
