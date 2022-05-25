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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TableSchema;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.IntegerType;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.JoinNode.EquiJoinClause;
import io.trino.sql.planner.plan.Patterns;
import io.trino.sql.planner.plan.ProjectNode;
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
import oshi.util.tuples.Pair;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.SystemSessionProperties.getSkewedJoinMetadata;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.plan.Patterns.Join.left;
import static io.trino.sql.planner.plan.Patterns.Join.right;
import static io.trino.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class SkewJoin
        implements Rule<JoinNode>
{
    private static final Capture<TableScanNode> LEFT_TABLE_SCAN = newCapture();
    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = newCapture();

    private static final Pattern<JoinNode> PATTERN =
            Patterns.join()
                    .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
                    .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));

    private Metadata metadata;

    public SkewJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    private Expression ifSkewed(SymbolReference keyColumn, List<Expression> skewedValues, Expression then, Expression otherWise)
    {
        return new IfExpression(
                new InPredicate(keyColumn, new InListExpression(skewedValues)),
                then,
                otherWise);
    }

    private ProjectNode projectProbeSide(TableScanNode probe, SymbolReference skewedLeftKey, List<Expression> skewedValues, Symbol randomPartSymbol, Context context)
    {
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();

        FunctionCall randomPartCall = FunctionCallBuilder.resolve(context.getSession(), this.metadata)
                .setName(QualifiedName.of("random"))
                .addArgument(IntegerType.INTEGER, new LongLiteral("2"))
                .build();

        Expression ifSkewedKey = ifSkewed(skewedLeftKey, skewedValues, randomPartCall, new LongLiteral("0"));

        Assignments assignments = Assignments.builder()
                .putIdentities(probe.getOutputSymbols())
                .put(randomPartSymbol, ifSkewedKey)
                .build();

        return new ProjectNode(idAllocator.getNextId(), probe, assignments);
    }

    private UnnestNode unnestBuildSide(TableScanNode build, SymbolReference skewedRightKey, List<Expression> skewedValues, Symbol partSymbol, Context context)
    {
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();

        Symbol partitionerSmbol = context.getSymbolAllocator().newSymbol("skewPartitioner", new ArrayType(IntegerType.INTEGER));
        ArrayConstructor partitioner = new ArrayConstructor(ImmutableList.of(new LongLiteral("0"), new LongLiteral("1"), new LongLiteral("2")));
        ArrayConstructor noop = new ArrayConstructor(ImmutableList.of(new LongLiteral("0")));

        Expression ifSkewedKey = ifSkewed(skewedRightKey, skewedValues, partitioner, noop);

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

    private JoinNode rewriteJoinClause(JoinNode node, ProjectNode leftSource, Symbol leftKey, UnnestNode rightSource, Symbol rightKey, Context context)
    {
        List<EquiJoinClause> criteria = Stream
                .concat(node.getCriteria().stream(), Stream.of(new JoinNode.EquiJoinClause(leftKey, rightKey)))
                .collect(Collectors.toUnmodifiableList());

        return new JoinNode(
                context.getIdAllocator().getNextId(),
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

    private ProjectNode dropPartSymbols(JoinNode node, ImmutableSet<Symbol> symbolsToRemove, Context context)
    {
        ImmutableList<Symbol> newOutputSymbols = node.getOutputSymbols().stream()
                .filter(symbol -> !symbolsToRemove.contains(symbol))
                .collect(toImmutableList());

        Assignments assignments = Assignments.builder().putIdentities(newOutputSymbols).build();

        return new ProjectNode(context.getIdAllocator().getNextId(), node, assignments);
    }

    private SymbolReference getSkewedRightKey(JoinNode joinNode, SymbolReference skewedLeftKey)
    {
        List<EquiJoinClause> criteria = joinNode.getCriteria();
        Optional<EquiJoinClause> skewedClause = criteria.stream().filter(clause -> clause.getLeft().toSymbolReference().equals(skewedLeftKey)).findFirst();
        return skewedClause.get().getRight().toSymbolReference();
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        final TableScanNode leftNode = captures.get(LEFT_TABLE_SCAN);
        final TableScanNode rightNode = captures.get(RIGHT_TABLE_SCAN);
        final String skewedTableName = leftNode.getTable().toString().replace(':', '.'); // TODO: Get this from metadata

        if (joinNode.getType() != JoinNode.Type.INNER && joinNode.getType() != JoinNode.Type.LEFT) {
            // for now we only support left and inner join since they won't cause row duplication.
            return Result.empty();
        }

        List<List<String>> skewedJoinMetadata = getSkewedJoinMetadata(context.getSession());

        Optional<List<String>> skewedTableMetadata = skewedJoinMetadata.stream()
                .filter(metadata -> metadata.get(0).equals(skewedTableName))
                .findFirst();

        if(skewedTableMetadata.isEmpty()) {
            // We couldn't find this table in the list of skewed tables the user supplied.
            return Result.empty();
        }

        final List<String> leftTableMetadata = skewedTableMetadata.get(); // Todo: validate this array
        final String skewedColumnName = leftTableMetadata.get(1);
        final List<String> skewedValues = leftTableMetadata.subList(2, leftTableMetadata.size());
        final Session session = context.getSession();
        final TableSchema schema = metadata.getTableSchema(context.getSession(), leftNode.getTable());

        Optional<Pair<Symbol, ColumnMetadata>> leftMetadata = leftNode.getAssignments().entrySet().stream()
                .map(entry -> {
                    ColumnMetadata meta = metadata.getColumnMetadata(session, leftNode.getTable(), entry.getValue());
                    return new Pair<>(entry.getKey(), meta);
                })
                .filter(pair -> pair.getB().getName().equals(skewedColumnName))
                .findFirst();

        if(leftMetadata.isEmpty()) {
            // The column supplied by the user did not match any of the columns in the table.
            return Result.empty();
        }

        final SymbolReference skewedLeftKey = leftMetadata.get().getA().toSymbolReference();
        final DataType skewedLeftType = toSqlType(leftMetadata.get().getB().getType());
        final SymbolReference skewedRightKey = getSkewedRightKey(joinNode, skewedLeftKey);
        final List<Expression> skewedExpressionValues = skewedValues.stream()
                .map(StringLiteral::new)
                .map(literal-> new Cast(literal, skewedLeftType))
                .collect(Collectors.toList());

        Symbol randomPartSymbol = context.getSymbolAllocator().newSymbol("randPart", IntegerType.INTEGER);
        Symbol partSymbol = context.getSymbolAllocator().newSymbol("skewPart", IntegerType.INTEGER);

        ProjectNode newLeftNode = projectProbeSide(leftNode, skewedLeftKey, skewedExpressionValues, randomPartSymbol, context);
        UnnestNode newRightNode = unnestBuildSide(rightNode, skewedRightKey, skewedExpressionValues, partSymbol, context);

        JoinNode newJoinNode = rewriteJoinClause(joinNode, newLeftNode, randomPartSymbol, newRightNode, partSymbol, context);
        ProjectNode dropSymbols = dropPartSymbols(newJoinNode, ImmutableSet.of(randomPartSymbol, partSymbol), context);

        return Result.ofPlanNode(dropSymbols);
    }
}
