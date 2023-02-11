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
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.FunctionCallBuilder;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.AggregationNode.GroupingSetDescriptor;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.tree.CoalesceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.StringLiteral;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.Patterns.aggregation;

public class FlattenMultiChannelAggregate
        implements Rule<AggregationNode>
{
    private final PlannerContext plannerContext;
    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;

    public FlattenMultiChannelAggregate(PlannerContext plannerContext, Metadata metadata, TypeAnalyzer typeAnalyzer)
    {
        this.plannerContext = plannerContext;
        this.metadata = metadata;
        this.typeAnalyzer = typeAnalyzer;
    }

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(agg -> agg.getGroupingKeys().size() > 1)
            .matching(agg -> agg.getGroupingSets().getGroupingSetCount() == 1);

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    public List<Expression> toExpression(List<Symbol> symbols)
    {
        //this is a hack, it will conflate the literal string "null" with NULL
        Expression nullLiteral = new StringLiteral("null");
        return symbols.stream()
                .map(symbol -> new CoalesceExpression(symbol.toSymbolReference(), nullLiteral))
                .collect(Collectors.toList());
    }

    public Expression flattenGroupingKeys(List<Type> keyTypes, List<Symbol> groupingKeys, Context context)
    {
        return FunctionCallBuilder.resolve(context.getSession(), plannerContext.getMetadata())
                .setName(QualifiedName.of("concat"))
                .setArguments(keyTypes, toExpression(groupingKeys))
                .build();
    }

    public Map<Symbol, Aggregation> addArbitraryAggregates(List<Symbol> groupingKeys, Map<Symbol, Aggregation> aggregates, Context context)
    {
        TypeProvider types = context.getSymbolAllocator().getTypes();
        Session session = context.getSession();

        Map<Symbol, Aggregation> arbitraryAggregates = groupingKeys.stream().collect(Collectors.toMap(key -> key,
                key -> {
                    ResolvedFunction func = metadata.resolveFunction(
                            session,
                            QualifiedName.of("arbitrary"),
                            fromTypes(types.get(key)));
                    return new Aggregation(
                            func,
                            ImmutableList.of(key.toSymbolReference()),
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                }));

        arbitraryAggregates.putAll(aggregates);
        return arbitraryAggregates;
    }

    public Type calculateConcatType(List<Type> groupingKeyTypes)
    {
        long lengthSum = groupingKeyTypes.stream()
                .map(type -> (VarcharType) type)
                .map(type -> (long) type.getLength().orElse(VarcharType.UNBOUNDED_LENGTH))
                .reduce(Long::sum)
                .orElse((long) VarcharType.UNBOUNDED_LENGTH);
        int maxLength = (int) Long.min(lengthSum, VarcharType.UNBOUNDED_LENGTH);
        return VarcharType.createVarcharType(maxLength);
    }

    public boolean isAllVarcharType(List<Type> groupingKeyTypes)
    {
        return groupingKeyTypes.stream()
                .map(key -> (key instanceof VarcharType))
                .reduce(Boolean::logicalAnd)
                .orElse(false);
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        TypeProvider typeProvider = context.getSymbolAllocator().getTypes();
        List<Symbol> groupingKeys = node.getGroupingKeys();
        List<Type> keyTypes = groupingKeys.stream().map(typeProvider::get).toList();

        if (!isAllVarcharType(keyTypes)) {
            return Result.empty();
        }

        Expression uniqueKeyExpr = flattenGroupingKeys(keyTypes, groupingKeys, context);
        //Type uniqueKeyType = calculateConcatType(keyTypes);
        Type uniqueKeyType = typeAnalyzer.getTypes(context.getSession(), context.getSymbolAllocator().getTypes(), uniqueKeyExpr).get(NodeRef.of(uniqueKeyExpr));

        Symbol uniqueKey = context.getSymbolAllocator().newSymbol("concatenatedKeys", uniqueKeyType);
        Assignments.Builder concatAssignments = Assignments.builder();
        concatAssignments.putIdentities(node.getSources().get(0).getOutputSymbols());
        concatAssignments.put(uniqueKey, uniqueKeyExpr);
        ProjectNode addConcat = new ProjectNode(context.getIdAllocator().getNextId(), node.getSources().get(0), concatAssignments.build());

        Map<Symbol, Aggregation> newAggregations = addArbitraryAggregates(node.getGroupingKeys(), node.getAggregations(), context);

        AggregationNode newAgg = AggregationNode.builderFrom(node)
                .setGroupingSets(new GroupingSetDescriptor(ImmutableList.of(uniqueKey), 1, ImmutableSet.of()))
                .setAggregations(newAggregations)
                .setSource(addConcat)
                .build();

        Assignments.Builder assignments = Assignments.builder();
        assignments.putIdentities(node.getOutputSymbols());
        ProjectNode dropConcat = new ProjectNode(context.getIdAllocator().getNextId(), newAgg, assignments.build());

        return Result.ofPlanNode(dropConcat);
    }
}
