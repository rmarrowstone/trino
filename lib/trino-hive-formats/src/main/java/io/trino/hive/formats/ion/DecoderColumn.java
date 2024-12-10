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

import java.util.List;
import java.util.Optional;

public record DecoderColumn(List<DecoderPathStep> path, Type type, int position)
{
    static DecoderColumn forFieldNamePath(List<String> path, Type type, int position)
    {
        return new DecoderColumn(path.stream().map(text -> (DecoderPathStep) new DecoderPathStep.FieldName(text)).toList(), type, position);
    }

    Optional<DecoderPathStep> head()
    {
        if (path.isEmpty()) {
            return Optional.empty();
        }
        else {
            return Optional.of(path.getFirst());
        }
    }

    DecoderColumn tail()
    {
        if (path.isEmpty()) {
            throw new IllegalArgumentException("Cannot take tail of empty path!");
        }
        return new DecoderColumn(path.subList(1, path.size()), type, position);
    }
}
