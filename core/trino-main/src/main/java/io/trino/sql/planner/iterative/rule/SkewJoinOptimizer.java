/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.sql.planner.iterative.rule;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.SimplePlanRewriter;
import io.trino.sql.planner.plan.TableScanNode;
import org.assertj.core.util.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.SystemSessionProperties.getSkewedJoinMetadata;

public class SkewJoinOptimizer
        implements PlanOptimizer
{
    Metadata metadata;

    public SkewJoinOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return null;
    }

    private class Rewriter
            extends SimplePlanRewriter<Map<Symbol, SkewJoinConfig>>
    {
        private final Session session;
        private final Metadata metadata;

        public Rewriter(Session session, Metadata metadata)
        {
            this.session = session;
            this.metadata = metadata;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Map<Symbol, SkewJoinConfig>> context)
        {
            PlanNode leftNode = node.getLeft().accept(this, context);
            PlanNode rightNode = node.getLeft().accept(this, context);
            Map<Symbol, SkewJoinConfig> skewedSymbols = context.get();
            node.getOutputSymbols().stream().map(symbol -> skewedSymbols.put(symbol, null));
            context.get().clear();
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Map<Symbol, SkewJoinConfig>> context)
        {
            final Map<Symbol, SkewJoinConfig> skewedSymbols = context.get();
            final String qualifiedTableName = metadata.getTableMetadata(session, node.getTable()).getQualifiedName().toString();
            final Optional<SkewJoinConfig> skewJoinConfig = SkewJoinConfig.getMetadata(qualifiedTableName, session);

            if(skewJoinConfig.isPresent()) {
                SkewJoinConfig skewConfig = skewJoinConfig.get();
                Optional<Map.Entry<Symbol, ColumnHandle>> symbolAndMetadata = node.getAssignments().entrySet().stream()
                        .filter(entry -> {
                            ColumnMetadata meta = metadata.getColumnMetadata(session, node.getTable(), entry.getValue());
                            return meta.getName().equals(skewConfig.getColumnName());
                        })
                        .findFirst();
                symbolAndMetadata.ifPresent(m -> skewedSymbols.put(m.getKey(), skewConfig));
            }

            return node;
        }
    }

    @VisibleForTesting
    static class SkewJoinConfig
    {
        /*
         * Parses and validates the user supplied session data.
         * */

        final String tableName;
        final String columnName;
        final List<String> values;

        private static final int TABLE_INDEX = 0;
        private static final int COLUMN_INDEX = 1;
        private static final int VALUE_INDEX = 2;
        private static final int MIN_SIZE = 3;

        private SkewJoinConfig(List<String> rawMetadata)
        {
            tableName = rawMetadata.get(TABLE_INDEX);
            columnName = rawMetadata.get(COLUMN_INDEX);
            values = rawMetadata.subList(VALUE_INDEX, rawMetadata.size());
        }

        public String getTableName()
        {
            return tableName;
        }

        public String getColumnName()
        {
            return columnName;
        }

        public List<String> getValues()
        {
            return values;
        }

        private static Optional<List<String>> getSkewedTableMetadata(String skewedTableName, Session session)
        {
            List<List<String>> skewedJoinMetadata = getSkewedJoinMetadata(session);

            return skewedJoinMetadata.stream()
                    .filter(metadata -> metadata.get(TABLE_INDEX).equals(skewedTableName))
                    .findFirst();
        }

        static Optional<SkewJoinConfig> getMetadata(String skewedTableName, Session session)
        {
            Optional<List<String>> rawMetadata = getSkewedTableMetadata(skewedTableName, session);

            if (rawMetadata.isEmpty() || rawMetadata.get().size() < MIN_SIZE) {
                return Optional.empty();
            }

            return Optional.of(new SkewJoinConfig(rawMetadata.get()));
        }
    }
}
