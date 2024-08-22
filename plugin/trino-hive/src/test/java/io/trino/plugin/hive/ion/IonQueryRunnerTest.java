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
