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

package org.apache.shardingsphere.elasticjob.cloud.console;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import org.apache.shardingsphere.elasticjob.cloud.config.pojo.CloudJobConfigurationPOJO;
import org.apache.shardingsphere.elasticjob.cloud.console.service.CloudAppService;
import org.apache.shardingsphere.elasticjob.cloud.console.service.CloudJobService;
import org.apache.shardingsphere.elasticjob.cloud.console.service.CloudOperationService;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.config.app.pojo.CloudAppConfigurationPOJO;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.env.RestfulServerConfiguration;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.exception.AppConfigurationException;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.mesos.ReconcileService;
import org.apache.shardingsphere.elasticjob.cloud.scheduler.producer.ProducerManager;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.netty.http.server.HttpServerRoutes;

import java.text.ParseException;
import java.util.ArrayList;

/**
 * Console bootstrap for Cloud.
 */
public class ConsoleBootstrap {
    
    private final int port;
    
    private DisposableServer httpServer;
    
    public ConsoleBootstrap(final CoordinatorRegistryCenter regCenter, final RestfulServerConfiguration config, final ProducerManager producerManager, final ReconcileService reconcileService) {
        this.port = config.getPort();
        CloudAppService.init(regCenter, producerManager);
        CloudJobService.init(regCenter, producerManager);
        CloudOperationService.init(regCenter, reconcileService);
    }
    
    /**
     * Startup RESTful server.
     */
    public void start() {
        httpServer = HttpServer.create().port(port)
                .route(routes -> {
                    addAppRoutes(routes);
                    addOperationRoutes(routes);
                    addJobRoutes(routes);
                }).bindNow();
    }
    
    private void addAppRoutes(final HttpServerRoutes routes) {
        CloudAppService appOperation = CloudAppService.getInstance();
        Gson gson = new GsonBuilder().serializeNulls().create();
        routes
                .post("/api/app", (request, response) -> response.sendString(request.receive().asString()
                        .map(bodyString -> {
                            try {
                                CloudAppConfigurationPOJO appConfigurationPOJO = gson.fromJson(bodyString, CloudAppConfigurationPOJO.class);
                                return String.valueOf(appOperation.register(appConfigurationPOJO));
                            } catch (AppConfigurationException ex) {
                                return ex.toString();
                            } catch (JsonSyntaxException ex) {
                                return ex.getMessage();
                            }
                        })
                ))
                .put("/api/app", (request, response) -> response.sendString(
                        request.receive().asString().map(bodyString -> {
                            CloudAppConfigurationPOJO appConfigurationPOJO = gson.fromJson(bodyString, CloudAppConfigurationPOJO.class);
                            return String.valueOf(appOperation.update(appConfigurationPOJO));
                        })
                ))
                .get("/api/app/list", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(appOperation.findAllApps())))
                )
                .get("/api/app/{appName}", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(appOperation.detail(request.param("appName")))))
                )
                .get("/api/app/{appName}/disable", (request, response) ->
                        response.sendString(Mono.just(String.valueOf(appOperation.isDisabled(request.param("appName")))))
                )
                .post("/api/app/{appName}/disable", (request, response) ->
                        response.sendString(Mono.just(String.valueOf(appOperation.disable(request.param("appName")))))
                )
                .post("/api/app/{appName}/enable", (request, response) ->
                        response.sendString(Mono.just(String.valueOf(appOperation.enable(request.param("appName")))))
                )
                .delete("/api/app/{appName}", (request, response) ->
                        response.sendString(Mono.just(gson.toJson(appOperation.deregister(request.param("appName"))))));
    }
    
    private void addOperationRoutes(final HttpServerRoutes routes) {
        CloudOperationService cloudOperationService = CloudOperationService.getInstance();
        Gson gson = new GsonBuilder().serializeNulls().create();
        routes
                .get("/api/operate/sandbox/{appName}", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(new ArrayList<>(cloudOperationService.sandbox(request.param("appName"))))))
                )
                .post("/api/operate/reconcile/explicit", (request, response) ->
                        response.sendString(Mono.just(String.valueOf(cloudOperationService.explicitReconcile())))
                )
                .post("/api/operate/reconcile/implicit", (request, response) ->
                        response.sendString(Mono.just(String.valueOf(cloudOperationService.implicitReconcile()))));
    }
    
    private void addJobRoutes(final HttpServerRoutes routes) {
        CloudJobService cloudJobService = CloudJobService.getInstance();
        Gson gson = new GsonBuilder().serializeNulls().create();
        routes
                .post("/api/job/register", (request, response) -> response.sendString(
                        request.receive().asString().map(bodyString -> {
                            try {
                                CloudJobConfigurationPOJO cloudJobConfigurationPOJO = gson.fromJson(bodyString, CloudJobConfigurationPOJO.class);
                                return gson.toJson(cloudJobService.register(cloudJobConfigurationPOJO));
                            } catch (AppConfigurationException ex) {
                                return ex.toString();
                            }
                        }))
                )
                .put("/api/job/update", (request, response) -> response.sendString(
                        request.receive().asString().map(bodyString -> {
                            CloudJobConfigurationPOJO cloudJobConfigurationPOJO = gson.fromJson(bodyString, CloudJobConfigurationPOJO.class);
                            return gson.toJson(cloudJobService.update(cloudJobConfigurationPOJO));
                        })
                ))
                .delete("/api/job/{jobName}/deregister", (request, response) ->
                        response.sendString(Mono.just(gson.toJson(cloudJobService.deregister(request.param("jobName")))))
                )
                .get("/api/job/{jobName}/disable", (request, response) ->
                        response.sendString(Mono.just(gson.toJson(cloudJobService.isDisabled(request.param("jobName")))))
                )
                .post("/api/job/{jobName}/enable", (request, response) ->
                        response.sendString(Mono.just(gson.toJson(cloudJobService.enable(request.param("jobName")))))
                )
                .post("/api/job/{jobName}/disable", (request, response) ->
                        response.sendString(Mono.just(gson.toJson(cloudJobService.disable(request.param("jobName")))))
                )
                .post("/api/job/trigger", (request, response) -> response.sendString(
                        request.receive().asString().map(jobName -> {
                            return gson.toJson(cloudJobService.trigger(jobName));
                        }))
                )
                .get("/api/job/jobs/{jobName}", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.detail(request.param("jobName")))))
                )
                .get("/api/job/jobs", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.findAllJobs())))
                )
                .get("/api/job/tasks/running", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.findAllRunningTasks())))
                )
                .get("/api/job/tasks/ready", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.findAllReadyTasks())))
                )
                .get("/api/job/tasks/failover", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.findAllFailoverTasks())))
                )
                .get("/api/job/events/executions", (request, response) -> {
                            try {
                                return response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                        .sendString(Mono.just(gson.toJson(cloudJobService.findJobExecutionEvents(request.params()))));
                            } catch (ParseException ex) {
                                return response.sendString(Mono.just(ex.toString()));
                            }
                        }
                )
                .get("/api/job/events/statusTraces", (request, response) -> {
                            try {
                                return response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                        .sendString(Mono.just(gson.toJson(cloudJobService.findJobStatusTraceEvents(request.params()))));
                            } catch (ParseException ex) {
                                return response.sendString(Mono.just(ex.toString()));
                            }
                        }
                )
                .get("/api/job/statistics/tasks/results", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.findTaskResultStatistics(request.param("since")))))
                )
                .get("/api/job/statistics/tasks/results/{period}", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.getTaskResultStatistics(request.param("period")))))
                )
                .get("/api/job/statistics/tasks/running/{since}", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.findTaskRunningStatistics(request.param("since")))))
                )
                .get("/api/job/statistics/jobs/executionType", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.getJobExecutionTypeStatistics())))
                )
                .get("/api/job/statistics/jobs/running/{since}", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(new ArrayList<>(cloudJobService.findJobRunningStatistics(request.param("since"))))))
                )
                .get("/api/job/statistics/jobs/register", (request, response) ->
                        response.addHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON)
                                .sendString(Mono.just(gson.toJson(cloudJobService.findJobRegisterStatistics()))));
    }
    
    /**
     * Stop RESTful server.
     */
    public void stop() {
        httpServer.dispose();
    }
}
