package io.trino.plugin.hive.ion;

import io.trino.plugin.hive.BaseHiveConnectorTest;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;

public class IonQueryRunnerTest
        extends BaseHiveConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHiveQueryRunner(HiveQueryRunner.builder());
    }

    @Test
    public void testRoundTrip()
    {
        String table = "test_ion_format_" + randomNameSuffix();
        assertUpdate("""
            create table %s (
                magic_num int,
                some_text varchar,
                is_summer boolean,
                sequencer array(int),
                struction row(foo int, bar varchar))
            with (format = 'ion')"""
                .formatted(table));

        assertUpdate("""
            insert into %s values (
                 17,
                'hello',
                true,
                array[1, 2, 3],
                row(31, 'goodbye')
            )""".formatted(table), 1);

        assertQuery("""
            select
                magic_num,
                some_text,
                is_summer,
                sequencer,
                -- the H2QueryRunner cannot handle Rows on output, so we'll project the kiddos
                struction.foo,
                struction.bar
            from %s""".formatted(table),
                "VALUES (17, 'hello', true, ARRAY[1, 2, 3], 31, 'goodbye')");
    }
}
