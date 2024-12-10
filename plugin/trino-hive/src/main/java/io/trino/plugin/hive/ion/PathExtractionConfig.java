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

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.hive.formats.ion.DecoderPathStep;
import io.trino.plugin.hive.HiveColumnHandle;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

class PathExtractionConfig
{
    private static final IonReaderBuilder readerBuilder = IonReaderBuilder.standard();

    private PathExtractionConfig() {}

    /**
     * Finds an Ion Path Extractor if one is defined in the table's serde properties.
     * If not found, a list containing the columnName is returned.
     */
    static Map<String, List<DecoderPathStep>> getIonPathToColumn(List<HiveColumnHandle> columns, Map<String, String> tableProperties)
    {
        ImmutableMap.Builder<String, List<DecoderPathStep>> extractionsBuilder = ImmutableMap.builder();

        for (HiveColumnHandle columnHandle : columns) {
            String columnName = columnHandle.getBaseColumnName();
            String extractorDef = tableProperties.get("ion.%s.path_extractor".formatted(columnName));
            if (extractorDef == null) {
                extractionsBuilder.put(columnName, List.of(new DecoderPathStep.FieldName(columnName)));
            }
            else {
                List<DecoderPathStep> path = parsePath(extractorDef);
                extractionsBuilder.put(columnName, path);
            }
        }

        // we may have multiple columns with the same base column name
        // they will have the same path
        return extractionsBuilder.buildKeepingLast();
    }

    /**
     * This copies the relatively simple logic of parsing paths from the path extractor
     * configs from the ion-java-path-extraction.
     */
    private static List<DecoderPathStep> parsePath(String path)
    {
        try (IonReader reader = readerBuilder.build(path)) {
            IonType containerType = reader.next();

            checkArgument(containerType != null, "search path must not be null");
            checkArgument(containerType.equals(IonType.LIST) || containerType.equals(IonType.SEXP),
                    "search path must be an Ion sequence");
            validateNoAnnotations(reader);

            reader.stepIn();
            ImmutableList.Builder<DecoderPathStep> builder = ImmutableList.builder();
            while (reader.next() != null) {
                validateNoAnnotations(reader);
                builder.add(parseStep(reader));
            }
            reader.stepOut();

            return builder.build();
        }
        catch (IOException e) {
            throw new IllegalStateException("not reachable", e);
        }
    }

    private static void validateNoAnnotations(IonReader reader)
    {
        if (reader.getTypeAnnotations().length > 0) {
            throw new UnsupportedPathException("annotation matching is not supported!");
        }
    }

    private static DecoderPathStep parseStep(IonReader reader)
    {
        return switch (reader.getType()) {
            case IonType.INT -> new DecoderPathStep.ListIndex(reader.intValue());
            case IonType.STRING, IonType.SYMBOL -> {
                String text = reader.stringValue();
                if (text.equals("*")) {
                    throw new UnsupportedPathException("Wildcard matching is not supported!");
                }
                yield new DecoderPathStep.FieldName(text);
            }
            default -> throw new IllegalArgumentException("Path step must be IonText (field name)");
        };
    }
}
