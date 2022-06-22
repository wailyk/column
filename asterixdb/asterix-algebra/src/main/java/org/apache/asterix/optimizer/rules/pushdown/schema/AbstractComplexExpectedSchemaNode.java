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
package org.apache.asterix.optimizer.rules.pushdown.schema;

import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.api.exceptions.SourceLocation;

public abstract class AbstractComplexExpectedSchemaNode extends AbstractExpectedSchemaNode {
    /**
     * @see #setRequestedEntirely()
     */
    private boolean requestedEntirely;

    AbstractComplexExpectedSchemaNode(AbstractComplexExpectedSchemaNode parent, SourceLocation sourceLocation,
            FunctionIdentifier functionIdentifier) {
        super(parent, sourceLocation, functionIdentifier);
        requestedEntirely = false;
    }

    @Override
    public IExpectedSchemaNode replaceIfNeeded(ExpectedSchemaNodeType expectedNodeType, SourceLocation sourceLocation,
            FunctionIdentifier functionIdentifier) {
        //If no change is required, return the same node
        IExpectedSchemaNode node = this;

        if (expectedNodeType == ExpectedSchemaNodeType.ANY) {
            /*
             * We want to fall back to ANY. This could happen if we needed one nested value in one expression.
             * But another expression request the entire node is needed. So, we fall back to ANY and remove any
             * information about the nested value. For example:
             * SELECT t.hashtags[*].text, t.hashtags
             * FROM Tweets t
             * In this case, we first saw (t.hashtags[*].text), but the next expression (t.hashtags) requested
             * the entire hashtags. So, the expected type for hashtags should be ANY
             */
            node = new AnyExpectedSchemaNode(getParent(), getSourceLocation(), getFunctionIdentifier());
            getParent().replaceChild(this, node);
        } else if (expectedNodeType != getType()) {
            /*
             * We need to change the type to UNION, as the same value was accessed as an ARRAY and as an OBJECT.
             * This is possible if we have heterogeneous value access in the query.
             */

            //Create UNION node and its parent is the parent of this
            UnionExpectedSchemaNode unionSchemaNode =
                    new UnionExpectedSchemaNode(getParent(), getSourceLocation(), getFunctionIdentifier());

            //Add this as a child of UNION
            unionSchemaNode.addChild(this);
            /*
             * Replace the reference of this in its parent with the union node
             * Before: parent --> this
             * After:  parent --> UNION --> this
             */
            getParent().replaceChild(this, unionSchemaNode);
            /*
             * Set the parent of this to union
             * Before: oldParent <-- this
             * After:  oldParent <-- UNION <-- this
             */
            setParent(unionSchemaNode);
            /*
             * Add the new child with the expected type to union
             * Before: UNION <-- this
             * After:  UNION <-- (this, newChild)
             */
            unionSchemaNode.createChild(expectedNodeType, sourceLocation, functionIdentifier);
            node = unionSchemaNode;
        }
        return node;
    }

    /**
     * In certain cases, we do not want to revert nodes to {@link ExpectedSchemaNodeType#ANY}. Instead, we want to
     * keep the inferred schema information for nested values. In those cases, calling this flag must replace the
     * call to {@link #replaceIfNeeded(ExpectedSchemaNodeType, SourceLocation, FunctionIdentifier)}.
     */
    public void setRequestedEntirely() {
        this.requestedEntirely = true;
    }

    /**
     * @return {@code true} if the entire nested node was requested, {@code false} otherwise
     */
    public boolean isRequestedEntirely() {
        return requestedEntirely;
    }

    protected abstract void replaceChild(IExpectedSchemaNode oldNode, IExpectedSchemaNode newNode);

    public static AbstractComplexExpectedSchemaNode createNestedNode(ExpectedSchemaNodeType type,
            AbstractComplexExpectedSchemaNode parent, SourceLocation sourceLocation,
            FunctionIdentifier functionIdentifier) {
        switch (type) {
            case ARRAY:
                return new ArrayExpectedSchemaNode(parent, sourceLocation, functionIdentifier);
            case OBJECT:
                return new ObjectExpectedSchemaNode(parent, sourceLocation, functionIdentifier);
            case UNION:
                return new UnionExpectedSchemaNode(parent, sourceLocation, functionIdentifier);
            default:
                throw new IllegalStateException(type + " is not nested or unknown");
        }
    }
}
