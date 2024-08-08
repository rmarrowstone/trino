package io.trino.plugin.hive.optimizer;

import io.trino.filesystem.Location;
import io.trino.plugin.hive.BaseHiveConnectorTest;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class JsonConnectorProjectionPushdownTest
    extends BaseHiveConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHiveQueryRunner(HiveQueryRunner.builder());
    }

    @Test
    public void testIt()
    {
        /**
         * here is what the below spits out from the SELECT with some print.errs in LinePageSourceFactory:
         * Input Columns:
         * 	basic#keeper_a:int:REGULAR
         * 	twice_projected#keeper_d:int:REGULAR
         * 	twice_projected#keeper_c:int:REGULAR
         * 	twice_nested#inner_kept#keeper_b:int:REGULAR
         * 'Base' Columns:
         * 	basic:struct<keeper_a:int,ignored_a:string>:REGULAR
         * 	twice_projected:struct<keeper_c:int,keeper_d:int,ignored_d:string>:REGULAR
         * 	twice_nested:struct<inner_kept:struct<keeper_b:int,ignored_b:string>,ignored_c:string>:REGULAR
         *
         * 	todo: projection where one "covers" the other, e.g.: a.b and a.b.c
         */

        String table = "test_projection_pushdown_" + randomNameSuffix();
        assertUpdate("""
            create table %s
                (basic row(keeper_a int, ignored_a varchar),
                twice_nested row(ignored_c varchar, inner_kept row(keeper_b int, ignored_b varchar)),
                twice_projected row(keeper_c int, ignored_d varchar, keeper_d int),
                ignored_e varchar)
            with (format = 'json')"""
                .formatted(table)); // with (format = 'JSON')
        assertUpdate("insert into %s values (row(17, 'a'), row('c', row(31, 'b')), row(51, 'd', 67), 'e')".formatted(table), 1);
        assertQuery("select basic.keeper_a, twice_nested.inner_kept.keeper_b, twice_projected.keeper_c, twice_projected.keeper_d from %s where 1 = 1".formatted(table),
                "VALUES (17, 31, 51, 67)");
    }
}
