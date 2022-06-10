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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.SortNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.sql.tree.ArrayConstructor;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.IfExpression;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.Table;
import org.assertj.core.util.VisibleForTesting;
import oshi.util.tuples.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getSkewedJoinMetadata;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.util.Objects.requireNonNull;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;

class SkewedContext {
    private TableScanNode table;
    public Set<Symbol> skewedSymbols;

    public SkewedContext() {
        this.table = null;
        this.skewedSymbols = new HashSet<>();
    }

    public void setTable(TableScanNode table) {
        this.table = table;
    }
}

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
        return new Rewriter(session, metadata).visitPlan(plan, new SkewedContext());
    }

    private class Rewriter extends PlanVisitor<PlanNode, SkewedContext>
    {
        private final Session session;
        private final Metadata metadata;

        public Rewriter(Session session, Metadata metadata)
        {
            this.session = session;
            this.metadata = metadata;
        }

        public PlanNode passThrough(PlanNode node, SkewedContext context) {
            PlanNode child = Iterables.getOnlyElement(node.getSources());
            PlanNode rewrittenChild = child.accept(this, context);
            return replaceChildren(node, ImmutableList.of(rewrittenChild));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, SkewedContext context)
        {
            SkewedContext leftCtx = new SkewedContext();
            SkewedContext rightCtx = new SkewedContext();
            PlanNode leftNode = node.getLeft().accept(this, leftCtx);
            PlanNode rightNode = node.getRight().accept(this, rightCtx);
            //node.getOutputSymbols().stream().map(symbol -> skewedSymbols.put(symbol, null));
            return node;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, SkewedContext context) {
            return passThrough(node, context);
        }

        @Override
        public PlanNode visitSort(SortNode node, SkewedContext context) {
            return passThrough(node, context);
        }

//        @Override
//        public PlanNode visitExchange(ExchangeNode node, SkewedContext context) {
//            // TODO
//            return null;
//        }

// Amogh's project
//        @Override
//        public PlanNode visitProject(ProjectNode node, Set<SimpleSkewedTableScan> context) {
//            context.forEach(table -> table.skewedSymbols = table.skewedSymbols.stream()
//                    .filter(node.getOutputSymbols()::contains)
//                    .collect(Collectors.toSet()));
//            // Remove tables that have no skewed symbols
//            context.removeAll(
//                    context.stream()
//                            .filter(table -> table.skewedSymbols.isEmpty())
//                            .collect(Collectors.toSet()));
//            return super.visitProject(node, context);
//        }

        @Override
        public PlanNode visitProject(ProjectNode node, SkewedContext context) {
            // select primary_key = primary_key % 23; x
            // select primary_key = primary_key + 'sds'; x
            // select primary_key as foo, ok
            // set skew_join_metadata = ARRAY['table', 'col', '1', '9']
            // todo check how alias is represented in the assignments
            PlanNode child = node.getSource().accept(this, context);
            node.getAssignments().entrySet()
                    .forEach(entry -> {
                        Expression value = entry.getValue();
                        if (entry.getValue() instanceof SymbolReference) {
                            if (context.skewedSymbols.contains(Symbol.from(value))) {
                                context.skewedSymbols.add(entry.getKey());
                            }
                        }
                    });
            return replaceChildren(node, ImmutableList.of(child));
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, SkewedContext context)
        {
            // for now passthrough the context
            List<PlanNode> children = node.getSources().stream().map(child -> child.accept(this, context)).collect(Collectors.toList());
            // create a new context for unsupported nodes like union.
            //List<PlanNode> children = node.getSources().stream().map(child -> child.accept(this, new SkewedContext())).collect(Collectors.toList());
            return replaceChildren(node, children);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, SkewedContext context)
        {
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
                symbolAndMetadata.ifPresent(m -> {context.skewedSymbols.add(m.getKey()); context.setTable(node);});
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

    class JoinRewriter
    {
        private final LongLiteral MAX_REPLICATION_FACTOR = new LongLiteral("2");
        private final LongLiteral DEFAULT_PROJECT_VALUE = new LongLiteral("0");
        private final ArrayConstructor ARRAY_PARTITIONER = new ArrayConstructor(ImmutableList.of(new LongLiteral("0"), new LongLiteral("1"), new LongLiteral("2")));
        private final ArrayConstructor DEFAULT_UNNEST_VALUE = new ArrayConstructor(ImmutableList.of(DEFAULT_PROJECT_VALUE));

        private Metadata metadata;
        private PlanNodeIdAllocator idAllocator;
        private SymbolAllocator symbolAllocator;
        private Session session;

        public JoinRewriter(Metadata metadata, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.idAllocator = requireNonNull(idAllocator, "id allocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbol allocator is null");
            this.session = requireNonNull(session, "session is null");
        }

        private Expression ifContains(SymbolReference keyColumn, List<Expression> skewedValues, Expression then, Expression otherWise)
        {
            return new IfExpression(
                    new InPredicate(keyColumn, new InListExpression(skewedValues)),
                    then,
                    otherWise);
        }

        private Optional<Pair<Symbol, ColumnMetadata>> getLeftSymbolAndMetadata(TableScanNode leftNode, String skewedColumnName)
        {
            return leftNode.getAssignments().entrySet().stream()
                    .map(entry -> {
                        ColumnMetadata meta = metadata.getColumnMetadata(session, leftNode.getTable(), entry.getValue());
                        return new Pair<>(entry.getKey(), meta);
                    })
                    .filter(pair -> pair.getB().getName().equals(skewedColumnName))
                    .findFirst();
        }

        private SymbolReference getRightSymbol(JoinNode joinNode, SymbolReference skewedLeftKey)
        {
            List<JoinNode.EquiJoinClause> criteria = joinNode.getCriteria();
            Optional<JoinNode.EquiJoinClause> skewedClause = criteria.stream().filter(clause -> clause.getLeft().toSymbolReference().equals(skewedLeftKey)).findFirst();
            return skewedClause.get().getRight().toSymbolReference();
        }

        private ProjectNode projectProbeSide(PlanNode probe, SymbolReference skewedLeftKey, List<Expression> inSkewedValues, Symbol randomPartSymbol)
        {
            FunctionCall randomPartCall = FunctionCallBuilder.resolve(session, this.metadata)
                    .setName(QualifiedName.of("random"))
                    .addArgument(IntegerType.INTEGER, MAX_REPLICATION_FACTOR)
                    .build();

            Expression ifSkewedKey = ifContains(skewedLeftKey, inSkewedValues, randomPartCall, DEFAULT_PROJECT_VALUE);

            Assignments assignments = Assignments.builder()
                    .putIdentities(probe.getOutputSymbols())
                    .put(randomPartSymbol, ifSkewedKey)
                    .build();

            return new ProjectNode(idAllocator.getNextId(), probe, assignments);
        }

        private UnnestNode unnestBuildSide(PlanNode build, SymbolReference skewedRightKey, List<Expression> inSkewedValues, Symbol partSymbol)
        {
            Symbol partitionerSmbol = symbolAllocator.newSymbol("skewPartitioner", new ArrayType(IntegerType.INTEGER));

            Expression ifSkewedKey = ifContains(skewedRightKey, inSkewedValues, ARRAY_PARTITIONER, DEFAULT_UNNEST_VALUE);

            Assignments assignments = Assignments.builder()
                    .putIdentities(build.getOutputSymbols())
                    .put(partitionerSmbol, ifSkewedKey)
                    .build();

            ProjectNode setPartitionArray = new ProjectNode(idAllocator.getNextId(), build, assignments);

            return new UnnestNode(
                    idAllocator.getNextId(),
                    setPartitionArray,
                    build.getOutputSymbols(),
                    ImmutableList.of(new UnnestNode.Mapping(partitionerSmbol, ImmutableList.of(partSymbol))),
                    Optional.empty(),
                    JoinNode.Type.INNER,
                    Optional.empty());
        }

        private JoinNode rewriteJoinClause(JoinNode node, ProjectNode leftSource, Symbol leftKey, UnnestNode rightSource, Symbol rightKey)
        {
            List<JoinNode.EquiJoinClause> criteria = Stream
                    .concat(node.getCriteria().stream(), Stream.of(new JoinNode.EquiJoinClause(leftKey, rightKey)))
                    .collect(Collectors.toUnmodifiableList());

            return new JoinNode(
                    idAllocator.getNextId(),
                    node.getType(),
                    leftSource,
                    rightSource,
                    criteria,
                    node.getLeftOutputSymbols(),
                    node.getRightOutputSymbols(),
                    node.isMaySkipOutputDuplicates(),
                    node.getFilter(),
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol(),
                    node.getDistributionType(),
                    node.isSpillable(),
                    node.getDynamicFilters(),
                    node.getReorderJoinStatsAndCost());
        }

        private ProjectNode dropAddedSymbols(JoinNode node, ImmutableSet<Symbol> symbolsToRemove)
        {
            ImmutableList<Symbol> newOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> !symbolsToRemove.contains(symbol))
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder().putIdentities(newOutputSymbols).build();

            return new ProjectNode(idAllocator.getNextId(), node, assignments);
        }

        public PlanNode apply(JoinNode joinNode, SymbolReference skewedLeftKey, DataType skewedLeftType, List<String> skewedValues)
        {
            if (joinNode.getDistributionType().isEmpty() || joinNode.getDistributionType().get() != JoinNode.DistributionType.PARTITIONED) {
                // Only optimize partitioned joins
                return joinNode;
            }

            if (joinNode.getType() != JoinNode.Type.INNER && joinNode.getType() != JoinNode.Type.LEFT) {
                // Only support left and inner joins since they won't cause row duplication.
                return joinNode;
            }

            final SymbolReference skewedRightKey = getRightSymbol(joinNode, skewedLeftKey);
            final List<Expression> skewedExpressionValues = skewedValues.stream()
                    .map(StringLiteral::new)
                    .map(literal -> new Cast(literal, skewedLeftType))
                    .collect(Collectors.toList());

            final Symbol randomPartSymbol = symbolAllocator.newSymbol("randPart", IntegerType.INTEGER);
            final Symbol partSymbol = symbolAllocator.newSymbol("skewPart", IntegerType.INTEGER);

            ProjectNode newLeftNode = projectProbeSide(joinNode.getLeft(), skewedLeftKey, skewedExpressionValues, randomPartSymbol);
            UnnestNode newRightNode = unnestBuildSide(joinNode.getRight(), skewedRightKey, skewedExpressionValues, partSymbol);

            JoinNode newJoinNode = rewriteJoinClause(joinNode, newLeftNode, randomPartSymbol, newRightNode, partSymbol);

            return dropAddedSymbols(newJoinNode, ImmutableSet.of(randomPartSymbol, partSymbol));
        }
    }
}
