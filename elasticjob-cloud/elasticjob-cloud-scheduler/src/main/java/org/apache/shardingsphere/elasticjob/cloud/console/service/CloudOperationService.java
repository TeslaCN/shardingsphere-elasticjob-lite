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

package org.apache.shardingsphere.elasticjob.cloud.console.service;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.JsonParseException;
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.mesos.MesosStateService;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.mesos.ReconcileService;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.Collection;
import java.util.Map;

/**
 * Cloud operation restful api.
 */
@Slf4j
public final class CloudOperationService {
    
    private static volatile CloudOperationService instance;
    
    private static ReconcileService reconcileService;
    
    private static final long RECONCILE_MILLIS_INTERVAL = 10 * 1000L;
    
    private static MesosStateService mesosStateService;
    
    private static long lastReconcileTime;
    
    /**
     * Init.
     *
     * @param regCenter        registry center
     * @param reconcileService reconcile service
     */
    public static void init(final CoordinatorRegistryCenter regCenter, final ReconcileService reconcileService) {
        CloudOperationService.reconcileService = reconcileService;
        CloudOperationService.mesosStateService = new MesosStateService(regCenter);
    }
    
    /**
     * Get singleton.
     *
     * @return instance
     */
    public static CloudOperationService getInstance() {
        if (null == instance) {
            synchronized (CloudOperationService.class) {
                if (null == instance) {
                    instance = new CloudOperationService();
                }
            }
        }
        return instance;
    }
    
    /**
     * Explicit reconcile service.
     *
     * @return operate result
     */
    public boolean explicitReconcile() {
        validReconcileInterval();
        reconcileService.explicitReconcile();
        return true;
    }
    
    /**
     * Implicit reconcile service.
     *
     * @return operate result
     */
    public boolean implicitReconcile() {
        validReconcileInterval();
        reconcileService.implicitReconcile();
        return true;
    }

    private void validReconcileInterval() {
        if (System.currentTimeMillis() < lastReconcileTime + RECONCILE_MILLIS_INTERVAL) {
            throw new RuntimeException("Repeat explicitReconcile");
        }
        lastReconcileTime = System.currentTimeMillis();
    }
    
    /**
     * Get sandbox of the cloud job by app name.
     *
     * @param appName application name
     * @return sandbox info
     * @throws JsonParseException parse json exception
     */
    public Collection<Map<String, String>> sandbox(final String appName) throws JsonParseException {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(appName), "Lack param 'appName'");
        return mesosStateService.sandbox(appName);
    }
}
