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

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.lite.api.bootstrap.impl.ScheduleJobBootstrap;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperConfiguration;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperRegistryCenter;

import java.util.Optional;

public final class ReproduceMain {
    
    public static void main(String[] args) {
        CoordinatorRegistryCenter registryCenter = setupRegCenterFromProperties();
        JobConfiguration jobConfiguration = JobConfiguration.newBuilder("schedule-job", 3).cron("0/30 * * * * ?").build();
        new ScheduleJobBootstrap(registryCenter, new ScheduleSimpleJob("foo",registryCenter), jobConfiguration).schedule();
    }
    
    public static CoordinatorRegistryCenter setupRegCenterFromProperties() {
        String servers = Optional.ofNullable(System.getProperty("zk.servers")).orElseThrow(() -> new IllegalArgumentException("Missing -Dzk.servers"));
        String namespace = Optional.ofNullable(System.getProperty("zk.namespace")).orElseThrow(() -> new IllegalArgumentException("Missing -Dzk.namespace"));
        ZookeeperConfiguration zookeeperConfiguration = new ZookeeperConfiguration(servers, namespace);
        ZookeeperRegistryCenter zookeeperRegistryCenter = new ZookeeperRegistryCenter(zookeeperConfiguration);
        zookeeperRegistryCenter.init();
        return zookeeperRegistryCenter;
    }
}
