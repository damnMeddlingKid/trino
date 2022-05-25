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
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
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
                .build();

        LocalQueryRunner queryRunner = LocalQueryRunner.create(defaultSession);

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
        // true result iff the condition is true

        tester().assertThat(new SkewJoin(tester().getMetadata()))
                .on(p -> p.join(
                        JoinNode.Type.INNER,
                        p.tableScan(tableHandleLeft, ImmutableList.of(p.symbol("column_0")), ImmutableMap.of(p.symbol("column_0"), left)),
                        p.tableScan(tableHandleRight, ImmutableList.of(p.symbol("column_1")), ImmutableMap.of(p.symbol("column_1"), right)),
                        new JoinNode.EquiJoinClause(p.symbol("column_0"), p.symbol("column_1"))))
                .matches(
                        project(
                                join(
                                        JoinNode.Type.INNER,
                                        ImmutableList.of(
                                                equiJoinClause("column_0", "column_1"),
                                                equiJoinClause("randpart", "skewpart")),
                                        project(
                                                tableScan("A", ImmutableMap.of("column_0", "column_0", "randpart", "randpart"))),
                                        unnest(
                                                project(
                                                        tableScan("B", ImmutableMap.of("column_1", "column_1")))))));
    }
}
