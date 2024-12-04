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
package io.trino.hive.formats.ion;

import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Models a tree of instructions to build an `IonDecoder` from.
 * Defines a single abstraction to capture both the ion path extractions and Trino dereferencing.
 */
public sealed interface DecoderInstructions
{
    record DecodeValue(Type type, Integer blockPosition)
            implements DecoderInstructions {}

    record StructFields(Map<String, DecoderInstructions> fields)
            implements DecoderInstructions
    {
        StructFields fold(String fieldName, DecoderColumn tail)
        {
            DecoderInstructions existing = fields.getOrDefault(fieldName, EMPTY);
            HashMap<String, DecoderInstructions> merged = new HashMap<>(fields);
            merged.put(fieldName, existing.fold(tail));

            return new StructFields(Collections.unmodifiableMap(merged));
        }
    }

    record Empty()
            implements DecoderInstructions {}

    Empty EMPTY = new Empty();
    StructFields EMPTY_STRUCT = new StructFields(Map.of());

    static DecoderInstructions forColumns(List<DecoderColumn> columns)
    {
        DecoderInstructions instructions = EMPTY;
        for (DecoderColumn column : columns) {
            instructions = instructions.fold(column);
        }

        return instructions;
    }

    /**
     * fold the `column` into this, returning a new DecoderInstructions which has merged
     * the new column into the instructions.
     */
    default DecoderInstructions fold(
            DecoderColumn column)
    {
        Optional<String> head = column.head();
        if (head.isEmpty()) {
            checkArgument(this instanceof Empty, "Cannot cover steps or other column with decoded column!");
            return new DecodeValue(column.type(), column.position());
        }

        String step = head.get();
        DecoderColumn tail = column.tail();
        return switch (this) {
            case Empty _ -> EMPTY_STRUCT.fold(step, tail);
            case StructFields struct -> struct.fold(step, tail);
            case DecodeValue _ -> throw new IllegalArgumentException("Cannot merge steps into covering decoded column!");
        };
    }
}
