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

package org.apache.shardingsphere.elasticjob.dag.job;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.tracing.api.TracingConfiguration;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public final class DagJobConfigurer {
    
    @Getter
    private final List<DagJobConfiguration> dagJobConfigurations = new LinkedList<>();
    
    @Getter
    private String name;
    
    /**
     * Set name.
     *
     * @param name Name
     * @return Configurer
     */
    public DagJobConfigurer name(final String name) {
        this.name = name;
        return this;
    }
    
    /**
     * New job configuration builder.
     *
     * @return Builder
     */
    public DagJobConfiguration.Builder newJob() {
        return DagJobConfiguration.newBuilder(this);
    }
    
    private void addJobConfiguration(final DagJobConfiguration dagJobConfiguration) {
        dagJobConfigurations.add(dagJobConfiguration);
    }
    
    @RequiredArgsConstructor
    @Getter
    public static class DagJobConfiguration {
        
        private final CoordinatorRegistryCenter registryCenter;
        
        private final String elasticJobType;
        
        private final ElasticJob elasticJob;
        
        private final JobConfiguration jobConfiguration;
        
        private final TracingConfiguration<?> tracing;
        
        private final Set<String> parents;
        
        /**
         * Create a builder.
         *
         * @param configurer Configurer
         * @return Builder
         */
        public static Builder newBuilder(final DagJobConfigurer configurer) {
            return new Builder(configurer);
        }
        
        @RequiredArgsConstructor
        public static class Builder {
            
            private final DagJobConfigurer configurer;
            
            private CoordinatorRegistryCenter registryCenter;
            
            private String elasticJobType;
            
            private ElasticJob elasticJob;
            
            private JobConfiguration jobConfiguration;
            
            private TracingConfiguration<?> tracing;
            
            private final Set<String> parents = new LinkedHashSet<>();
            
            /**
             * Registry center.
             *
             * @param registryCenter RegistryCenter
             * @return Builder
             */
            public Builder registryCenter(final CoordinatorRegistryCenter registryCenter) {
                this.registryCenter = registryCenter;
                return this;
            }
            
            /**
             * Elastic job type.
             *
             * @param elasticJobType ElasticJobType
             * @return Builder
             */
            public Builder elasticJobType(final String elasticJobType) {
                this.elasticJobType = elasticJobType;
                return this;
            }
            
            /**
             * Elastic job.
             *
             * @param elasticJob ElasticJob
             * @return Builder
             */
            public Builder elasticJob(final ElasticJob elasticJob) {
                this.elasticJob = elasticJob;
                return this;
            }
            
            /**
             * Job configuration.
             *
             * @param jobConfiguration JobConfiguration
             * @return Builder
             */
            public Builder jobConfiguration(final JobConfiguration jobConfiguration) {
                this.jobConfiguration = jobConfiguration;
                return this;
            }
            
            /**
             * TracingConfiguration.
             *
             * @param tracing TracingConfiguration
             * @return Builder
             */
            public Builder tracing(final TracingConfiguration<?> tracing) {
                this.tracing = tracing;
                return this;
            }
            
            /**
             * Parent.
             *
             * @param parent Parent
             * @return Builder
             */
            public Builder parent(final String parent) {
                if (!Strings.isNullOrEmpty(parent)) {
                    parents.add(parent);
                }
                return this;
            }
            
            private DagJobConfiguration build() {
                return new DagJobConfiguration(registryCenter, elasticJobType, elasticJob, jobConfiguration, tracing, parents);
            }
            
            /**
             * Configuration finish.
             *
             * @return Configurer
             */
            public DagJobConfigurer end() {
                configurer.addJobConfiguration(build());
                return configurer;
            }
        }
    }
}
