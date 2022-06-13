package io.trino.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.CatalogName;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.SymbolAllocator;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.assertions.PlanAssert;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.SkewJoinOptimizer;
import io.trino.sql.planner.iterative.rule.test.PlanBuilder;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.testing.TestingTransactionHandle;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;

@Test(singleThreaded = true)
public class TestSkewJoinOptimizer extends BasePlanTest
{
    private PlannerContext plannerContext;
    private Metadata metadata;
    private PlanBuilder builder;
    private Symbol lineitemOrderKeySymbol;
    private TableScanNode lineitemTableScanNode;
    private TableHandle lineitemTableHandle;
    private Symbol ordersOrderKeySymbol;
    private TableScanNode ordersTableScanNode;

    public TestSkewJoinOptimizer() {
        super(ImmutableMap.of("skewed_join_metadata", "[[\"local.sf1.orders\", \"ORDERS_OK\", \"1\", \"2\"]]"));
    }

    @BeforeClass
    public void setup() {
        plannerContext = getQueryRunner().getPlannerContext();
        metadata = plannerContext.getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata, TEST_SESSION);
        CatalogName catalogName = getCurrentConnectorId();
        lineitemTableHandle = new TableHandle(
                catalogName,
                new TpchTableHandle("sf1", "lineitem", 1.0),
                TestingTransactionHandle.create());
        lineitemOrderKeySymbol = builder.symbol("LINEITEM_OK", BIGINT);
        lineitemTableScanNode = builder.tableScan(lineitemTableHandle, ImmutableList.of(lineitemOrderKeySymbol), ImmutableMap.of(lineitemOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));

        TableHandle ordersTableHandle = new TableHandle(
                catalogName,
                new TpchTableHandle("sf1", "orders", 1.0),
                TestingTransactionHandle.create());
        ordersOrderKeySymbol = builder.symbol("ORDERS_OK", BIGINT);
        ordersTableScanNode = builder.tableScan(ordersTableHandle, ImmutableList.of(ordersOrderKeySymbol), ImmutableMap.of(ordersOrderKeySymbol, new TpchColumnHandle("orderkey", BIGINT)));
    }

    @Test
    public void testSkewJoinSql()
    {
        assertPlan("SELECT o.orderkey FROM orders o LEFT JOIN lineitem l ON l.orderkey = o.orderkey",
                project(
                        join(
                                JoinNode.Type.INNER,
                                ImmutableList.of(
                                        equiJoinClause("ORDERS_OK", "LINEITEM_OK"),
                                        equiJoinClause("randPart", "skewPart")),
                                project(ImmutableMap.of("randPart", expression("IF((\"ORDERS_OK\" IN (CAST('1' AS bigint), CAST('2' AS bigint))), random(2), 0)"), "ORDERS_OK", expression("ORDERS_OK")),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                unnest(
                                        ImmutableList.of("LINEITEM_OK"),
                                        ImmutableList.of(unnestMapping("skewPartitioner", ImmutableList.of("skewPart"))),
                                        project(ImmutableMap.of("skewPartitioner", expression("IF((\"LINEITEM_OK\" IN (CAST('1' AS bigint), CAST('2' AS bigint))), ARRAY[0,1,2], ARRAY[0])"), "LINEITEM_OK", expression("LINEITEM_OK")),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    @Test
    public void testSkewJoin()
    {
        PlanNode actualQuery = builder.join(
                JoinNode.Type.INNER,
                ordersTableScanNode,
                lineitemTableScanNode,
                ImmutableList.of(new JoinNode.EquiJoinClause(ordersOrderKeySymbol, lineitemOrderKeySymbol)),
                ordersTableScanNode.getOutputSymbols(),
                lineitemTableScanNode.getOutputSymbols(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinNode.DistributionType.PARTITIONED),
                ImmutableMap.of()
        );

        assertPlan(optimizeSkewedJoins(actualQuery),
                project(
                        join(
                                JoinNode.Type.INNER,
                                ImmutableList.of(
                                        equiJoinClause("ORDERS_OK", "LINEITEM_OK"),
                                        equiJoinClause("randPart", "skewPart")),
                                project(ImmutableMap.of("randPart", expression("IF((\"ORDERS_OK\" IN (CAST('1' AS bigint), CAST('2' AS bigint))), random(2), 0)"), "ORDERS_OK", expression("ORDERS_OK")),
                                        tableScan("orders", ImmutableMap.of("ORDERS_OK", "orderkey"))),
                                unnest(
                                        ImmutableList.of("LINEITEM_OK"),
                                        ImmutableList.of(unnestMapping("skewPartitioner", ImmutableList.of("skewPart"))),
                                        project(ImmutableMap.of("skewPartitioner", expression("IF((\"LINEITEM_OK\" IN (CAST('1' AS bigint), CAST('2' AS bigint))), ARRAY[0,1,2], ARRAY[0])"), "LINEITEM_OK", expression("LINEITEM_OK")),
                                                tableScan("lineitem", ImmutableMap.of("LINEITEM_OK", "orderkey")))))));
    }

    private PlanNode optimizeSkewedJoins(PlanNode root)
    {
        return getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanNode rewrittenPlan = new SkewJoinOptimizer(metadata).optimize(root, session, builder.getTypes(), new SymbolAllocator(), new PlanNodeIdAllocator(), WarningCollector.NOOP);
            return rewrittenPlan;
        });
    }

    void assertPlan(PlanNode actual, PlanMatchPattern pattern)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            PlanAssert.assertPlan(
                    session,
                    metadata,
                    getQueryRunner().getFunctionManager(),
                    getQueryRunner().getStatsCalculator(),
                    new Plan(actual, builder.getTypes(), StatsAndCosts.empty()),
                    pattern);
            return null;
        });
    }
}
