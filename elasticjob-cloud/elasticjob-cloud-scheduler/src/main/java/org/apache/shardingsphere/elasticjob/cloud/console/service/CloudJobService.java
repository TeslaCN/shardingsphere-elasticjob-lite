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
import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.cloud.config.CloudJobExecutionType;
import org.apache.shardingsphere.elasticjob.cloud.config.pojo.CloudJobConfigurationPOJO;
import org.apache.shardingsphere.elasticjob.cloud.console.service.search.JobEventRdbSearch;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.config.job.CloudJobConfigurationService;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.env.BootstrapEnvironment;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.mesos.FacadeService;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.producer.ProducerManager;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.state.failover.FailoverTaskInfo;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.statistics.StatisticManager;
import org.apache.shardingsphere.elasticjob.cloud.statistics.StatisticInterval;
import org.apache.shardingsphere.elasticjob.cloud.statistics.type.job.JobExecutionTypeStatistics;
import org.apache.shardingsphere.elasticjob.cloud.statistics.type.job.JobRegisterStatistics;
import org.apache.shardingsphere.elasticjob.cloud.statistics.type.job.JobRunningStatistics;
import org.apache.shardingsphere.elasticjob.cloud.statistics.type.task.TaskResultStatistics;
import org.apache.shardingsphere.elasticjob.cloud.statistics.type.task.TaskRunningStatistics;
import org.apache.shardingsphere.elasticjob.infra.context.TaskContext;
import org.apache.shardingsphere.elasticjob.infra.exception.JobSystemException;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.tracing.api.TracingConfiguration;
import org.apache.shardingsphere.elasticjob.tracing.event.JobExecutionEvent;
import org.apache.shardingsphere.elasticjob.tracing.event.JobStatusTraceEvent;

import javax.sql.DataSource;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Cloud job restful api.
 */
@Slf4j
public final class CloudJobService {
    
    private static volatile CloudJobService instance;
    
    private static CoordinatorRegistryCenter regCenter;
    
    private static JobEventRdbSearch jobEventRdbSearch;
    
    private static ProducerManager producerManager;
    
    private final CloudJobConfigurationService configService;
    
    private final FacadeService facadeService;
    
    private final StatisticManager statisticManager;
    
    private CloudJobService() {
        Preconditions.checkNotNull(regCenter);
        configService = new CloudJobConfigurationService(regCenter);
        facadeService = new FacadeService(regCenter);
        statisticManager = StatisticManager.getInstance(regCenter, null);
    }
    
    /**
     * Init.
     * @param regCenter       registry center
     * @param producerManager producer manager
     */
    public static void init(final CoordinatorRegistryCenter regCenter, final ProducerManager producerManager) {
        CloudJobService.regCenter = regCenter;
        CloudJobService.producerManager = producerManager;
        Optional<TracingConfiguration> tracingConfiguration = BootstrapEnvironment.getINSTANCE().getTracingConfiguration();
        jobEventRdbSearch = tracingConfiguration.map(tracingConfiguration1 -> new JobEventRdbSearch((DataSource) tracingConfiguration1.getStorage())).orElse(null);
    }
    
    /**
     * Get instance.
     *
     * @return instance
     */
    public static CloudJobService getInstance() {
        if (null == instance) {
            synchronized (CloudJobService.class) {
                if (null == instance) {
                    instance = new CloudJobService();
                }
            }
        }
        return instance;
    }
    
    /**
     * Register cloud job.
     *
     * @param cloudJobConfig cloud job configuration
     * @return result
     */
    public boolean register(final CloudJobConfigurationPOJO cloudJobConfig) {
        producerManager.register(cloudJobConfig);
        return true;
    }
    
    /**
     * Update cloud job.
     *
     * @param cloudJobConfig cloud job configuration
     * @return result
     */
    public boolean update(final CloudJobConfigurationPOJO cloudJobConfig) {
        producerManager.update(cloudJobConfig);
        return true;
    }
    
    /**
     * Deregister cloud job.
     *
     * @param jobName job name
     * @return result
     */
    public boolean deregister(final String jobName) {
        producerManager.deregister(jobName);
        return true;
    }
    
    /**
     * Check whether the cloud job is disabled or not.
     *
     * @param jobName job name
     * @return true is disabled, otherwise not
     */
    public boolean isDisabled(final String jobName) {
        return facadeService.isJobDisabled(jobName);
    }
    
    /**
     * Enable cloud job.
     *
     * @param jobName job name
     * @return result
     */
    public boolean enable(final String jobName) {
        Optional<CloudJobConfigurationPOJO> configOptional = configService.load(jobName);
        if (configOptional.isPresent()) {
            facadeService.enableJob(jobName);
            producerManager.reschedule(jobName);
            return true;
        }
        return false;
    }
    
    /**
     * Disable cloud job.
     *
     * @param jobName job name
     * @return result
     */
    public boolean disable(final String jobName) {
        if (configService.load(jobName).isPresent()) {
            facadeService.disableJob(jobName);
            producerManager.unschedule(jobName);
            return true;
        }
        return false;
    }
    
    /**
     * Trigger job once.
     *
     * @param jobName job name
     * @return result
     */
    public boolean trigger(final String jobName) {
        Optional<CloudJobConfigurationPOJO> config = configService.load(jobName);
        if (config.isPresent() && CloudJobExecutionType.DAEMON == config.get().getJobExecutionType()) {
            throw new JobSystemException("Daemon job '%s' cannot support trigger.", jobName);
        }
        facadeService.addTransient(jobName);
        return true;
    }
    
    /**
     * Query job detail.
     *
     * @param jobName job name
     * @return the job detail
     */
    public CloudJobConfigurationPOJO detail(final String jobName) {
        Optional<CloudJobConfigurationPOJO> cloudJobConfig = configService.load(jobName);
        return cloudJobConfig.orElse(null);
    }

    /**
     * Find all jobs.
     * @return all jobs
     */
    public Collection<CloudJobConfigurationPOJO> findAllJobs() {
        return configService.loadAll();
    }

    /**
     * Find all running tasks.
     * @return all running tasks
     */
    public Collection<TaskContext> findAllRunningTasks() {
        List<TaskContext> result = new LinkedList<>();
        for (Set<TaskContext> each : facadeService.getAllRunningTasks().values()) {
            result.addAll(each);
        }
        return result;
    }

    /**
     * Find all ready tasks.
     * @return collection of all ready tasks
     */
    public Collection<Map<String, String>> findAllReadyTasks() {
        Map<String, Integer> readyTasks = facadeService.getAllReadyTasks();
        List<Map<String, String>> result = new ArrayList<>(readyTasks.size());
        for (Map.Entry<String, Integer> each : readyTasks.entrySet()) {
            Map<String, String> oneTask = new HashMap<>(2, 1);
            oneTask.put("jobName", each.getKey());
            oneTask.put("times", String.valueOf(each.getValue()));
            result.add(oneTask);
        }
        return result;
    }

    /**
     * Find all failover tasks.
     * @return collection of all the failover tasks
     */
    public Collection<FailoverTaskInfo> findAllFailoverTasks() {
        List<FailoverTaskInfo> result = new LinkedList<>();
        for (Collection<FailoverTaskInfo> each : facadeService.getAllFailoverTasks().values()) {
            result.addAll(each);
        }
        return result;
    }
    
    /**
     * Find job execution events.
     *
     * @param requestParams request params
     * @return job execution event
     * @throws ParseException parse exception
     */
    public JobEventRdbSearch.Result<JobExecutionEvent> findJobExecutionEvents(final Map<String, String> requestParams) throws ParseException {
        if (!isRdbConfigured()) {
            return new JobEventRdbSearch.Result<>(0, Collections.emptyList());
        }
        return jobEventRdbSearch.findJobExecutionEvents(buildCondition(requestParams, new String[]{"jobName", "taskId", "ip", "isSuccess"}));
    }
    
    /**
     * Find job status trace events.
     *
     * @param requestParams request params
     * @return job status trace event
     * @throws ParseException parse exception
     */
    public JobEventRdbSearch.Result<JobStatusTraceEvent> findJobStatusTraceEvents(final Map<String, String> requestParams) throws ParseException {
        if (!isRdbConfigured()) {
            return new JobEventRdbSearch.Result<>(0, Collections.emptyList());
        }
        return jobEventRdbSearch.findJobStatusTraceEvents(buildCondition(requestParams, new String[]{"jobName", "taskId", "slaveId", "source", "executionType", "state"}));
    }
    
    private boolean isRdbConfigured() {
        return null != jobEventRdbSearch;
    }
    
    private JobEventRdbSearch.Condition buildCondition(final Map<String, String> requestParams, final String[] params) throws ParseException {
        int perPage = 10;
        int page = 1;
        if (!Strings.isNullOrEmpty(requestParams.get("per_page"))) {
            perPage = Integer.parseInt(requestParams.get("per_page"));
        }
        if (!Strings.isNullOrEmpty(requestParams.get("page"))) {
            page = Integer.parseInt(requestParams.get("page"));
        }
        String sort = requestParams.get("sort");
        String order = requestParams.get("order");
        Date startTime = null;
        Date endTime = null;
        Map<String, Object> fields = getQueryParameters(requestParams, params);
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (!Strings.isNullOrEmpty(requestParams.get("startTime"))) {
            startTime = simpleDateFormat.parse(requestParams.get("startTime"));
        }
        if (!Strings.isNullOrEmpty(requestParams.get("endTime"))) {
            endTime = simpleDateFormat.parse(requestParams.get("endTime"));
        }
        return new JobEventRdbSearch.Condition(perPage, page, sort, order, startTime, endTime, fields);
    }
    
    private Map<String, Object> getQueryParameters(final Map<String, String> requestParams, final String[] params) {
        final Map<String, Object> result = new HashMap<>();
        for (String each : params) {
            if (!Strings.isNullOrEmpty(requestParams.get(each))) {
                result.put(each, requestParams.get(each));
            }
        }
        return result;
    }
    
    /**
     * Find task result statistics.
     *
     * @param since time span
     * @return task result statistics
     */
    public List<TaskResultStatistics> findTaskResultStatistics(final String since) {
        if ("last24hours".equals(since)) {
            return statisticManager.findTaskResultStatisticsDaily();
        } else {
            return Collections.emptyList();
        }
    }
    
    /**
     * Get task result statistics.
     *
     * @param period time period
     * @return task result statistics
     */
    public TaskResultStatistics getTaskResultStatistics(final String period) {
        switch (period) {
            case "online":
                return statisticManager.getTaskResultStatisticsSinceOnline();
            case "lastWeek":
                return statisticManager.getTaskResultStatisticsWeekly();
            case "lastHour":
                return statisticManager.findLatestTaskResultStatistics(StatisticInterval.HOUR);
            case "lastMinute":
                return statisticManager.findLatestTaskResultStatistics(StatisticInterval.MINUTE);
            default:
                return new TaskResultStatistics(0, 0, StatisticInterval.DAY, new Date());
        }
    }
    
    /**
     * Find task running statistics.
     *
     * @param since time span
     * @return task result statistics
     */
    public List<TaskRunningStatistics> findTaskRunningStatistics(final String since) {
        if ("lastWeek".equals(since)) {
            return statisticManager.findTaskRunningStatisticsWeekly();
        } else {
            return Collections.emptyList();
        }
    }
    
    /**
     * Get job execution type statistics.
     * @return job execution statistics
     */
    public JobExecutionTypeStatistics getJobExecutionTypeStatistics() {
        return statisticManager.getJobExecutionTypeStatistics();
    }
    
    /**
     * Find job running statistics in the recent week.
     *
     * @param since time span
     * @return collection of job running statistics in the recent week
     */
    public List<JobRunningStatistics> findJobRunningStatistics(final String since) {
        if ("lastWeek".equals(since)) {
            return statisticManager.findJobRunningStatisticsWeekly();
        } else {
            return Collections.emptyList();
        }
    }
    
    /**
     * Find job register statistics.
     * @return collection of job register statistics since online
     */
    public List<JobRegisterStatistics> findJobRegisterStatistics() {
        return statisticManager.findJobRegisterStatisticsSinceOnline();
    }
}
