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
package org.apache.asterix.optimizer.rules.codegen;

import java.util.Set;

import org.apache.asterix.common.config.DatasetConfig.DatasetFormat;
import org.apache.asterix.common.config.DatasetConfig.DatasetType;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.metadata.declared.DataSource;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.optimizer.base.AsterixOptimizationContext;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalOperator;
import org.apache.hyracks.algebricks.core.algebra.base.IOptimizationContext;
import org.apache.hyracks.algebricks.core.rewriter.base.IAlgebraicRewriteRule;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectSet;

public class PlanToCodeGenRule implements IAlgebraicRewriteRule {
    private boolean run = false;

    @Override
    public boolean rewritePre(Mutable<ILogicalOperator> opRef, IOptimizationContext context)
            throws AlgebricksException {
        if (run || !context.getPhysicalOptimizationConfig().isCodeGen()) {
            return false;
        }
        run = shouldRun(context);
        if (run) {
            OperatorCodeGeneratorVisitor visitor = new OperatorCodeGeneratorVisitor(context);
            visitor.getGeneratedCode(opRef.getValue());
            run = true;
        }
        //TODO return true if things changed
        return true;
    }

    /**
     * Check whether the plan contains an external dataset that supports pushdown
     *
     * @param context optimization context
     * @return true if the plan contains such dataset, false otherwise
     */
    private boolean shouldRun(IOptimizationContext context) throws AlgebricksException {
        ObjectSet<Entry<Set<DataSource>>> entrySet =
                ((AsterixOptimizationContext) context).getDataSourceMap().int2ObjectEntrySet();
        MetadataProvider metadataProvider = (MetadataProvider) context.getMetadataProvider();
        for (Int2ObjectMap.Entry<Set<DataSource>> dataSources : entrySet) {
            for (DataSource dataSource : dataSources.getValue()) {
                if (isColumnarDataset(metadataProvider, dataSource)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isColumnarDataset(MetadataProvider metadataProvider, DataSource dataSource)
            throws AlgebricksException {
        DataverseName dataverse = dataSource.getId().getDataverseName();
        String datasetName = dataSource.getId().getDatasourceName();
        Dataset dataset = metadataProvider.findDataset(dataverse, datasetName);
        return dataset != null && dataset.getDatasetType() == DatasetType.INTERNAL
                && dataset.getDatasetFormatInfo().getFormat() == DatasetFormat.COLUMN;
    }
}
