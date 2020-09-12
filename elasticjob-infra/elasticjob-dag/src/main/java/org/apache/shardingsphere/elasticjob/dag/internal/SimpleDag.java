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
import org.apache.shardingsphere.elasticjob.dag.Dag;
import org.apache.shardingsphere.elasticjob.dag.Node;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

@RequiredArgsConstructor
public final class SimpleDag implements Dag {
    
    private final Map<String, Node> nodes = new LinkedHashMap<>();
    
    private final String id;
    
    @Override
    public String id() {
        return id;
    }
    
    @Override
    public void addNodes(final Node... nodes) {
        for (Node each : nodes) {
            if (this.nodes.containsKey(each.id())) {
                throw new IllegalStateException("Duplicate id");
            }
            this.nodes.put(each.id(), each);
        }
    }
    
    @Override
    public Node getNode(final String id) {
        return nodes.get(id);
    }
    
    @Override
    public Collection<Node> getNodes() {
        return nodes.values();
    }
    
    @Override
    public void run() {
        // TODO check if cyclic
        new SimpleDagInstance(this).run();
    }
}
