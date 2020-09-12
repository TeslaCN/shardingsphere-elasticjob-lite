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

package org.apache.shardingsphere.elasticjob.dag;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class Node {
    
    private final String id;
    
    private final Executable executable;
    
    private final Set<String> parents = new LinkedHashSet<>();
    
    public Node(final String id, final Executable executable, final Node... parents) {
        this(id, executable, Arrays.stream(parents).map(Node::id).collect(Collectors.toSet()));
    }
    
    public Node(final String id, final Executable executable, final Set<String> parents) {
        this.id = id;
        this.executable = executable;
        this.parents.addAll(parents);
    }
    
    /**
     * Node ID.
     *
     * @return ID
     */
    public String id() {
        return id;
    }
    
    /**
     * Parents of node.
     *
     * @return Parents
     */
    public Set<String> parents() {
        return Collections.unmodifiableSet(parents);
    }
    
    /**
     * Run node.
     */
    public void run() {
        executable.execute();
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Node node = (Node) o;
        return id.equals(node.id);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
