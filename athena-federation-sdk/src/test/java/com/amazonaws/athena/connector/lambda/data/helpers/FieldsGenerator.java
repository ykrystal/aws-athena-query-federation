/*-
 * #%L
 * Amazon Athena Query Federation SDK
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connector.lambda.data.helpers;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import net.jqwik.api.*;

import java.util.List;

// Commented out known failures with TODO's. Will need to go through each one and resolve.

public class FieldsGenerator {

    private int counter = 0; // use to guarentee field names are unique for structs
    private boolean DEFAULT_NULLABLE = false; // TODO: resolve and set to true
    private int MAX_RECURSION_DEPTH = 5;

    @Provide
    public Arbitrary<Field> field() {
        return field(0, null, DEFAULT_NULLABLE);
    }

    @Provide
    private Arbitrary<Field> field(int depth, String name, boolean nullable) {
        return
            fieldType(depth+1, nullable).flatMap(fieldType ->
                fieldChildren(fieldType, depth+1).flatMap(children -> {
                    String fieldName = name;
                    if (name == null) {
                        counter++;
                        fieldName = Arbitraries.strings().withCharRange('a', 'z').
                            ofMinLength(5).ofMaxLength(10).
                            sample() + counter;
                    }
                    return Arbitraries.just(new Field(fieldName, fieldType, children));
                })
            );
    }

    @Provide
    private Arbitrary<FieldType> primitiveFieldType(boolean nullable) {
        return Arbitraries.of(
            new FieldType(nullable, new ArrowType.Binary(), null)
            , new FieldType(nullable, new ArrowType.Bool(), null)
            , new FieldType(nullable, new ArrowType.Date(DateUnit.DAY), null)
            // TODO: Resolve and uncomment
            //, new FieldType(nullable, new ArrowType.Date(DateUnit.MILLISECOND), null)
            , new FieldType(nullable, new ArrowType.Decimal(10, 5, 128), null)
            , new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null)
            , new FieldType(nullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null)
            , new FieldType(nullable, new ArrowType.Int(32, true), null)
            , new FieldType(nullable, new ArrowType.Int(32, false), null)
            // TODO: Resolve and uncomment
            //, new FieldType(nullable, new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"), null)
            // TODO: Resolve and uncomment
            //, new FieldType(nullable, new ArrowType.Utf8(), null)
        );
    }

    @Provide
    private Arbitrary<FieldType> complexFieldType(boolean nullable) {
        return Arbitraries.of(
            new FieldType(nullable, new ArrowType.List(), null)
            , new FieldType(nullable, new ArrowType.Struct(), null)
            // TODO: Resolve and uncomment
            //, new FieldType(nullable, new ArrowType.Map(true), null)
        );
    }

    @Provide
    private Arbitrary<FieldType> fieldType(int depth, boolean nullable) {
        if (depth >= MAX_RECURSION_DEPTH ) {
            return primitiveFieldType(nullable);
        }

        return Arbitraries.oneOf(
            primitiveFieldType(nullable)
            , complexFieldType(nullable)
        );
    }

    @Provide
    private Arbitrary<java.util.List<Field>> fieldChildren(FieldType parentType, int depth) {
        if (parentType.getType().equals(ArrowType.List.INSTANCE)) {
            return field(depth, null, DEFAULT_NULLABLE).list().ofSize(1);
        }

        if (parentType.getType().equals(ArrowType.Struct.INSTANCE)) {
            return field(depth, null, DEFAULT_NULLABLE).list().uniqueElements().ofMinSize(1).ofMaxSize(3);
        }

        if (parentType.getType().getTypeID().equals(ArrowType.Map.TYPE_TYPE)) {
            Arbitrary<Field> keyField = field(depth, MapVector.KEY_NAME, false);
            Arbitrary<Field> valueField = field(depth, MapVector.VALUE_NAME, DEFAULT_NULLABLE);

            FieldType structType = new FieldType(false, ArrowType.Struct.INSTANCE, null);
            Arbitrary<List<Field>> structField = Combinators.combine(keyField, valueField).as((key, value) ->
                new Field(MapVector.DATA_VECTOR_NAME,
                    structType,
                    List.of(key, value)
            )).list().ofSize(1);
            return structField;
        }

        return Arbitraries.just((java.util.List<Field>) null);
    }
}
