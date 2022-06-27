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
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
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
import org.assertj.core.util.VisibleForTesting;
import oshi.util.tuples.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getSkewedJoinMetadata;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static java.util.Objects.requireNonNull;
import static io.trino.sql.planner.plan.ChildReplacer.replaceChildren;

class SkewedContext
{
    public final DataType keyType;
    public final List<String> values;

    public SkewedContext(List<String> values, DataType keyType)
    {
        this.keyType = keyType;
        this.values = values;
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
        List<List<String>> skewedJoinMetadata = getSkewedJoinMetadata(session);

        if (skewedJoinMetadata.size() == 0) {
            // No session information found, nothing to do.
            return plan;
        }

        Map<String, SkewJoinConfig> skewedTables = skewedJoinMetadata.stream()
                .map(SkewJoinConfig::getMetadata)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toMap(SkewJoinConfig::getTableName, Function.identity()));

        JoinRewriter rewriter = new JoinRewriter(metadata, idAllocator, symbolAllocator, session);

        return plan.accept(new PlanRewriter(session, metadata, skewedTables, rewriter), new HashMap<>());
    }

    static class PlanRewriter
            extends PlanVisitor<PlanNode, Map<Symbol, SkewedContext>>
    {
        private final Session session;
        private final Metadata metadata;
        private final JoinRewriter joinRewriter;
        private final Map<String, SkewJoinConfig> skewedTables;

        public PlanRewriter(Session session, Metadata metadata, Map<String, SkewJoinConfig> skewedTables, JoinRewriter rewriter)
        {
            this.session = session;
            this.metadata = metadata;
            this.skewedTables = skewedTables;
            this.joinRewriter = rewriter;
        }

        public PlanNode passThrough(PlanNode node, Map<Symbol, SkewedContext> context)
        {
            List<PlanNode> rewrittenChildren = node.getSources().stream()
                    .map(child -> child.accept(this, context))
                    .collect(Collectors.toList());
            return replaceChildren(node, rewrittenChildren);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, Map<Symbol, SkewedContext> context)
        {
            // TODO : how do range joins behave ?
            Map<Symbol, SkewedContext> leftCtx = new HashMap<>();
            Map<Symbol, SkewedContext> rightCtx = new HashMap<>();

            PlanNode leftReWritten = node.getLeft().accept(this, leftCtx);
            PlanNode rightReWritten = node.getRight().accept(this, rightCtx);

            JoinNode newJoin = this.joinRewriter.rewriteJoin(node, leftReWritten, rightReWritten, Stream.empty());

            if (leftCtx.size() > 0) {
                Optional<JoinNode.EquiJoinClause> maybeClause = newJoin.getCriteria().stream()
                        .filter(clause -> leftCtx.containsKey(clause.getLeft()))
                        .findFirst();

                if (maybeClause.isPresent()) {
                    JoinNode.EquiJoinClause skewedClause = maybeClause.get();
                    return this.joinRewriter.rewriteSkewedJoin(newJoin, skewedClause, leftCtx.get(skewedClause.getLeft()));
                }
            }

            return newJoin;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Map<Symbol, SkewedContext> context)
        {
            return passThrough(node, context);
        }

        @Override
        public PlanNode visitSort(SortNode node, Map<Symbol, SkewedContext> context)
        {
            return passThrough(node, context);
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, Map<Symbol, SkewedContext> context)
        {
            return passThrough(node, context);
        }

// Amogh's project, i believe this removes symbols from the context if they are not in the project. not sure if neeeded, lets discuss
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
        public PlanNode visitProject(ProjectNode node, Map<Symbol, SkewedContext> context)
        {
            /*
                If any symbol assignments refers to a skewed symbol then add that symbol to the set of skewed symbols.
            * */
            PlanNode child = node.getSource().accept(this, context);

            // Symbols that will be removed by this projection.
            Set<Symbol> outputSymbols = new HashSet<>(node.getOutputSymbols());
            Set<Symbol> symbolsToRemove = new HashSet<>(node.getSource().getOutputSymbols());
            symbolsToRemove.removeAll(outputSymbols);

            // Add all symbols that refer to a skewed symbol.
            node.getAssignments().entrySet()
                    .forEach(entry -> {
                        Expression value = entry.getValue();
                        if (entry.getValue() instanceof SymbolReference) {
                            Symbol skewedSymbol = Symbol.from(value);
                            if (context.containsKey(Symbol.from(value)) && !context.containsKey(entry.getKey())) {
                                context.put(entry.getKey(), context.get(skewedSymbol));
                            }
                        }
                    });

            // Remove symbols that are dropped in the projection
            context.keySet().removeAll(symbolsToRemove);

            return replaceChildren(node, ImmutableList.of(child));
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Map<Symbol, SkewedContext> context)
        {
            // For all other nodes don't let the context pass through.
            List<PlanNode> children = node.getSources().stream()
                    .map(child -> child.accept(this, new HashMap<>()))
                    .collect(Collectors.toList());
            return replaceChildren(node, children);
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, Map<Symbol, SkewedContext> context)
        {
            final String qualifiedTableName = metadata.getTableMetadata(session, node.getTable()).getQualifiedName().toString();

            if (this.skewedTables.containsKey(qualifiedTableName)) {
                SkewJoinConfig skewConfig = this.skewedTables.get(qualifiedTableName);
                Optional<Pair<Symbol, ColumnMetadata>> symbolAndMetadata = node.getAssignments().entrySet().stream()
                        .map(entry -> new Pair<>(entry.getKey(), metadata.getColumnMetadata(session, node.getTable(), entry.getValue())))
                        .filter(entry -> entry.getB().getName().equals(skewConfig.getColumnName()))
                        .findFirst();
                symbolAndMetadata.ifPresent(m -> {
                    context.putIfAbsent(m.getA(), new SkewedContext(skewConfig.values, toSqlType(m.getB().getType())));
                });
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
            values = ImmutableList.copyOf(rawMetadata.subList(VALUE_INDEX, rawMetadata.size()));
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

        static Optional<SkewJoinConfig> getMetadata(List<String> rawMetadata)
        {
            if (rawMetadata.size() < MIN_SIZE) {
                return Optional.empty();
            }

            return Optional.of(new SkewJoinConfig(rawMetadata));
        }
    }

    static class JoinRewriter
    {
        // TODO make this configurable
        private final LongLiteral MAX_REPLICATION_FACTOR = new LongLiteral("2");
        private final LongLiteral DEFAULT_PROJECT_VALUE = new LongLiteral("0");
        private final ArrayConstructor ARRAY_PARTITIONER = new ArrayConstructor(ImmutableList.of(new LongLiteral("0"), new LongLiteral("1"), new LongLiteral("2")));
        private final ArrayConstructor DEFAULT_UNNEST_VALUE = new ArrayConstructor(ImmutableList.of(DEFAULT_PROJECT_VALUE));

        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Session session;

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

        private ProjectNode dropAddedSymbols(JoinNode node, ImmutableSet<Symbol> symbolsToRemove)
        {
            ImmutableList<Symbol> newOutputSymbols = node.getOutputSymbols().stream()
                    .filter(symbol -> !symbolsToRemove.contains(symbol))
                    .collect(toImmutableList());

            Assignments assignments = Assignments.builder().putIdentities(newOutputSymbols).build();

            return new ProjectNode(idAllocator.getNextId(), node, assignments);
        }

        private JoinNode rewriteJoinClause(JoinNode node, ProjectNode leftSource, Symbol leftKey, UnnestNode rightSource, Symbol rightKey)
        {
            return rewriteJoin(node, leftSource, rightSource, Stream.of(new JoinNode.EquiJoinClause(leftKey, rightKey)));
        }

        public JoinNode rewriteJoin(JoinNode node, PlanNode leftSource, PlanNode rightSource, Stream<JoinNode.EquiJoinClause> additionalClause)
        {
            List<JoinNode.EquiJoinClause> criteria = Stream
                    .concat(node.getCriteria().stream(), additionalClause)
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

        public PlanNode rewriteSkewedJoin(JoinNode joinNode, JoinNode.EquiJoinClause skewedClause, SkewedContext context)
        {
            if (joinNode.getDistributionType().isEmpty() || joinNode.getDistributionType().get() != JoinNode.DistributionType.PARTITIONED) {
                // Only optimize partitioned joins
                return joinNode;
            }

            if (joinNode.getType() != JoinNode.Type.INNER && joinNode.getType() != JoinNode.Type.LEFT) {
                // Only support left and inner joins since they won't cause row duplication.
                return joinNode;
            }

            final SymbolReference skewedLeftKey = skewedClause.getLeft().toSymbolReference();
            final SymbolReference skewedRightKey = skewedClause.getRight().toSymbolReference();
            final List<Expression> skewedExpressionValues = context.values.stream()
                    .map(StringLiteral::new)
                    .map(literal -> new Cast(literal, context.keyType))
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
