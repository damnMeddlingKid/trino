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
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.CatalogName;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.MockConnectorTransactionHandle;
import io.trino.metadata.TableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.UnnestNode;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.UnnestMapping.unnestMapping;
import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.unnest;

public class TestSkewJoin
        extends BaseRuleTest
{
    private static final CatalogName TEST_CATALOG = new CatalogName("test_push_dl_catalog");
    private TableHandle tableHandleLeft;
    private TableHandle tableHandleRight;
    private ColumnHandle left;
    private ColumnHandle right;

    @BeforeClass
    public void init()
    {
        left = new MockConnectorColumnHandle("column_0", VarcharType.VARCHAR);
        right = new MockConnectorColumnHandle("column_1", VarcharType.VARCHAR);
        tableHandleLeft = new TableHandle(
                TEST_CATALOG,
                new MockConnectorTableHandle(new SchemaTableName("left", "A"), TupleDomain.all(), Optional.of(ImmutableList.of(left))),
                MockConnectorTransactionHandle.INSTANCE);
        tableHandleRight = new TableHandle(
                TEST_CATALOG,
                new MockConnectorTableHandle(new SchemaTableName("right", "B"), TupleDomain.all(), Optional.of(ImmutableList.of(right))),
                MockConnectorTransactionHandle.INSTANCE);
    }

    @Override
    protected Optional<LocalQueryRunner> createLocalQueryRunner()
    {
        Session defaultSession = TestingSession.testSessionBuilder()
                .setCatalog(TEST_CATALOG.getCatalogName())
                .setSchema("tiny")
                .setSystemProperty("skewed_join_metadata", "[[\"test_push_dl_catalog.left.a\", \"column_0\", \"1\", \"2\"]]")
                .build();

        LocalQueryRunner.Builder runnerBuilder = LocalQueryRunner.builder(defaultSession);
        LocalQueryRunner queryRunner = runnerBuilder.build();

        queryRunner.createCatalog(
                TEST_CATALOG.getCatalogName(),
                MockConnectorFactory.builder()
                        .build(),
                Map.of());

        return Optional.of(queryRunner);
    }

    @Test
    public void testSkewJoin()
    {
        tester().assertThat(new SkewJoin(tester().getMetadata()))
                .on(p -> {
                    PlanNode leftTableScan = p.tableScan(tableHandleLeft, ImmutableList.of(p.symbol("column_0")), ImmutableMap.of(p.symbol("column_0"), left));
                    PlanNode rightTableScan = p.tableScan(tableHandleRight, ImmutableList.of(p.symbol("column_1")), ImmutableMap.of(p.symbol("column_1"), right));
                    return p.join(
                            JoinNode.Type.INNER,
                            leftTableScan,
                            rightTableScan,
                            ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("column_0"), p.symbol("column_1"))),
                            leftTableScan.getOutputSymbols(),
                            rightTableScan.getOutputSymbols(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(JoinNode.DistributionType.PARTITIONED),
                            ImmutableMap.of()
                    );
                })
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(
                                                equiJoinClause("column_0", "column_1"),
                                                equiJoinClause("randPart", "skewPart")),
                                        project(ImmutableMap.of("randPart", expression("IF((\"column_0\" IN (CAST('1' AS varchar), CAST('2' AS varchar))), random(2), 0)"), "column_0", expression("column_0")),
                                                tableScan("A", ImmutableMap.of("column_0", "column_0"))),
                                        unnest(
                                                ImmutableList.of("column_1"),
                                                ImmutableList.of(unnestMapping("skewPartitioner", ImmutableList.of("skewPart"))),
                                                project(ImmutableMap.of("skewPartitioner", expression("IF((\"column_1\" IN (CAST('1' AS varchar), CAST('2' AS varchar))), ARRAY[0,1,2], ARRAY[0])"), "column_1", expression("column_1")),
                                                        tableScan("B", ImmutableMap.of("column_1", "column_1")))))));
    }
}
