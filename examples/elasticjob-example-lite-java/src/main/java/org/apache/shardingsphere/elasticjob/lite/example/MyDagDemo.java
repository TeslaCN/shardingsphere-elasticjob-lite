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

import com.google.common.base.Strings;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.lite.api.bootstrap.impl.ScheduleJobBootstrap;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperConfiguration;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperRegistryCenter;
import org.apache.shardingsphere.elasticjob.tracing.api.TracingConfiguration;

import javax.sql.DataSource;

public final class MyDagDemo {
    
    private static final int EMBED_ZOOKEEPER_PORT = 4181;
    
    private static final String ZOOKEEPER_CONNECTION_STRING = "localhost:" + EMBED_ZOOKEEPER_PORT;
    
    private static final String JOB_NAMESPACE = "elasticjob-dag";
    
    // switch to MySQL by yourself
//    private static final String EVENT_RDB_STORAGE_DRIVER = "com.mysql.jdbc.Driver";
//    private static final String EVENT_RDB_STORAGE_URL = "jdbc:mysql://localhost:3306/elastic_job_log";
    
    private static final String EVENT_RDB_STORAGE_DRIVER = "org.h2.Driver";
    private static final String EVENT_RDB_STORAGE_URL = "jdbc:h2:mem:job_event_storage";
    private static final String EVENT_RDB_STORAGE_USERNAME = "sa";
    private static final String EVENT_RDB_STORAGE_PASSWORD = "";
    
    // CHECKSTYLE:OFF
    public static void main(final String[] args) throws Exception {
        // CHECKSTYLE:ON
        CoordinatorRegistryCenter regCenter;
        final String servers = System.getProperty("zk.servers");
        if (Strings.isNullOrEmpty(servers)) {
            EmbedZookeeperServer.start(EMBED_ZOOKEEPER_PORT);
            regCenter = setUpRegistryCenter(ZOOKEEPER_CONNECTION_STRING);
        } else {
            regCenter = setUpRegistryCenter(servers);
        }
        TracingConfiguration<DataSource> tracingConfig = new TracingConfiguration<>("RDB", setUpEventTraceDataSource());
        setUpDagJob(regCenter, tracingConfig);
    }
    
    private static CoordinatorRegistryCenter setUpRegistryCenter(final String serverLists) {
        ZookeeperConfiguration zkConfig = new ZookeeperConfiguration(serverLists, JOB_NAMESPACE);
        zkConfig.setMaxRetries(30);
        CoordinatorRegistryCenter result = new ZookeeperRegistryCenter(zkConfig);
        result.init();
        return result;
    }
    
    private static DataSource setUpEventTraceDataSource() {
        BasicDataSource result = new BasicDataSource();
        result.setDriverClassName(EVENT_RDB_STORAGE_DRIVER);
        result.setUrl(EVENT_RDB_STORAGE_URL);
        result.setUsername(EVENT_RDB_STORAGE_USERNAME);
        result.setPassword(EVENT_RDB_STORAGE_PASSWORD);
        return result;
    }
    
    private static void setUpDagJob(final CoordinatorRegistryCenter regCenter, final TracingConfiguration<DataSource> tracingConfiguration) {
        final JobConfiguration jobConfig = JobConfiguration.newBuilder("my-dag-job", 10).cron("0/30 * * * * ?").overwrite(true).build();
        new ScheduleJobBootstrap(regCenter, new MyDagConfiguration(regCenter), jobConfig, tracingConfiguration).schedule();
    }
}
