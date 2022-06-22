/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.optimizer.rules.codegen.node.expression.literal;

import org.apache.asterix.optimizer.rules.codegen.node.AbstractCodeNode;
import org.apache.asterix.optimizer.rules.codegen.node.CodeNodeType;
import org.apache.asterix.optimizer.rules.codegen.node.ICodeNodeVisitor;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.SourceLocation;

public class IdentifierCodeNode extends AbstractCodeNode {
    private static final long serialVersionUID = 2493176756436907237L;
    private final String identifier;

    public IdentifierCodeNode(String identifier) {
        this(GENERATED_LOCATION, identifier);

    }

    public IdentifierCodeNode(SourceLocation sourceLocation, String identifier) {
        super(sourceLocation);
        this.identifier = identifier;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IdentifierCodeNode)) {
            return false;
        }
        IdentifierCodeNode other = (IdentifierCodeNode) obj;
        return identifier.equals(other.identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    @Override
    public String toString() {
        return identifier;
    }

    @Override
    public <R, T> R accept(ICodeNodeVisitor<R, T> visitor, T arg) throws AlgebricksException {
        return visitor.visit(this, arg);
    }

    @Override
    public CodeNodeType getType() {
        return CodeNodeType.IDENTIFIER;
    }
}
