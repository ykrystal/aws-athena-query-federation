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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableDateDayHolder;
import org.apache.arrow.vector.holders.NullableDateMilliHolder;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableTimeStampMilliTZHolder;
import org.apache.arrow.vector.holders.NullableUInt4Holder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;

import net.jqwik.api.*;
import net.jqwik.time.api.*;

import java.math.BigDecimal;
import java.time.MonthDay;

import static org.assertj.core.api.Assertions.*;

public class ValuesGenerator {

    public FieldVector generateValues(Field field) {
        FieldVector vector = field.createVector(new RootAllocator());
        generateValues(field, vector, 0);
        return vector;
    }

    public int generateValues(Field field, FieldVector vector, int length) {
        switch (vector.getMinorType()) {
            case BIT:
                return setBit(field, vector, length);
            case DATEDAY:
                return setDateDay(field, vector, length);
            case DATEMILLI:
                return setDateMilli(field, vector, length);
            case DECIMAL:
                return setDecimal(field, vector, length);
            case FLOAT4:
                return setFloat4(field, vector, length);
            case FLOAT8:
                return setFloat8(field, vector, length);
            case INT:
                return setInt(field, vector, length);
            case LIST:
                return setList(field, vector, length);
            case MAP:
                return setMap(field, vector, length);
            case STRUCT:
                return setStruct(field, vector, length);
            case TIMESTAMPMILLITZ:
                return setTimestampMilliTz(field, vector, length);
            case UINT4:
            return setUint4(field, vector, length);
            case VARBINARY:
                return setVarBinary(field, vector, length);
            case VARCHAR:
                return setVarChar(field, vector, length);
            default:
                throw new RuntimeException("Not yet implemented for typeId " + vector.getMinorType());
        }
    }

    private int getLength(int length) {
        if (length == 0) {
            length = Arbitraries.integers().between(1, 5).sample();
        }
        return length;
    }

    private boolean isNull(Field field) {
        if (!field.isNullable()) {
            return false;
        }

        // randomly determine if the value should be null or not
        boolean isNull = Arbitraries.integers().between(0, 5).sample() == 0 ? true : false;
        if (!isNull) {
            return false;
        }

        return true;
    }

    /*
     * Complex Types
     */

    private int setStruct(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();
        int maxChildSize = 0;

        for (int i = 0; i < vector.getChildrenFromFields().size(); i++) {
            Field childField = field.getChildren().get(i);
            FieldVector childVector = vector.getChildrenFromFields().get(i);
            maxChildSize = Math.max(generateValues(childField, childVector, length), maxChildSize);
        }

        for (int i = position; i < maxChildSize; i++) {
            ((StructVector) vector).setIndexDefined(i);
        }
        vector.setValueCount(maxChildSize);
        return vector.getValueCount();
    }

    private int setList(Field field, FieldVector vector, int length) {
        assertThat(vector.getChildrenFromFields().size()).isEqualTo(1);

        int position = vector.getValueCount();
        Field childField = field.getChildren().get(0);
        FieldVector childVector = vector.getChildrenFromFields().get(0);
        length = getLength(length);

        int prevChildSize = childVector.getValueCount();
        for (int i = 0; i < length; i++) {
            if (!isNull(field)) {
                ((ListVector) vector).startNewValue(position + i);
                int newChildSize = generateValues(childField, childVector, 0);
                ((ListVector) vector).endValue(position + i, newChildSize - prevChildSize);
                prevChildSize = newChildSize;
            }
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setMap(Field field, FieldVector vector, int length) {
        assertThat(vector.getChildrenFromFields().size()).isEqualTo(1);

        int position = vector.getValueCount();

        Field entriesField = field.getChildren().get(0);
        FieldVector entries = vector.getChildrenFromFields().get(0);

        assertThat(entries.getChildrenFromFields().size()).isEqualTo(2);
        Field keysField = entriesField.getChildren().get(0);
        FieldVector keys = entries.getChildrenFromFields().get(0);
        Field valuesField = entriesField.getChildren().get(1);
        FieldVector values = entries.getChildrenFromFields().get(1);

        length = getLength(length);

        for (int i = 0; i < length; i++) {
            ((MapVector) vector).startNewValue(position+i);
            int keysValuesLength = getLength(0);
            generateValues(keysField, keys, keysValuesLength);
            generateValues(valuesField, values, keysValuesLength);
            ((MapVector) vector).endValue(position+i, keysValuesLength);
        }
        vector.setValueCount(position+length);

        for (int i = 0; i < keys.getValueCount(); i++) {
            ((StructVector) entries).setIndexDefined(i);
        }
        entries.setValueCount(keys.getValueCount());
        return vector.getValueCount();
    }

    /*
     * Primitive Types
     */

    private int setInt(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((IntVector) vector).setSafe(i, new NullableIntHolder());
            }
            else {
                int rand = Arbitraries.integers().sample();
                ((IntVector) vector).setSafe(i, rand);
            }
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setVarChar(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((VarCharVector) vector).setSafe(i, new NullableVarCharHolder());
            }
            else {
                String rand = Arbitraries.strings().withCharRange('a', 'z')
                    .ofMinLength(5)
                    .ofMaxLength(10)
                    .sample();
                ((VarCharVector) vector).setSafe(i, rand.getBytes());
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setDateDay(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((DateDayVector) vector).setSafe(i, new NullableDateDayHolder());
            }
            else {
                MonthDay rand = Dates.monthDays().sample();
                ((DateDayVector) vector).setSafe(i, rand.getDayOfMonth());
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setDateMilli(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)){
                ((DateMilliVector) vector).setSafe(i, new NullableDateMilliHolder());
            }
            else {
                Long rand = Arbitraries.longs().sample();
                ((DateMilliVector) vector).setSafe(i, rand);
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setTimestampMilliTz(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((TimeStampMilliTZVector) vector).setSafe(i, new NullableTimeStampMilliTZHolder());
            } else {
                Long rand = Arbitraries.longs().greaterOrEqual(0).sample();
                ((TimeStampMilliTZVector) vector).setSafe(i, rand);
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setFloat4(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((Float4Vector) vector).setSafe(i, new NullableFloat4Holder());
            } else {
                Float rand = Arbitraries.floats().greaterOrEqual(0).sample();
                ((Float4Vector) vector).setSafe(i, rand);
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setFloat8(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((Float8Vector) vector).setSafe(i, new NullableFloat8Holder());
            } else {
                Double rand = Arbitraries.doubles().greaterOrEqual(0).sample();
                ((Float8Vector) vector).setSafe(i, rand);
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setVarBinary(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((VarBinaryVector) vector).setSafe(i, new NullableVarBinaryHolder());
            }
            else {
                String rand = Arbitraries.strings().sample();
                ((VarBinaryVector) vector).setSafe(i, rand.getBytes());
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setBit(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((BitVector) vector).setSafe(i, new NullableBitHolder());
            }
            else {
                int rand = Arbitraries.integers().between(0, 1).sample();
                ((BitVector) vector).setSafe(i, rand);
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setDecimal(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((DecimalVector) vector).setSafe(i, new NullableDecimalHolder());
            } else {
                BigDecimal rand = Arbitraries.bigDecimals()
                .between(new BigDecimal("-99999"), new BigDecimal("99999"))
                .ofScale(5).sample();
                ((DecimalVector) vector).setSafe(i, rand);
            }
        }

        vector.setValueCount(position + length);
        return vector.getValueCount();
    }

    private int setUint4(Field field, FieldVector vector, int length) {
        int position = vector.getValueCount();

        length = getLength(length);

        for (int i = position; i < position + length; i++) {
            if (isNull(field)) {
                ((UInt4Vector) vector).setSafe(i, new NullableUInt4Holder());
            } else {
                int rand = Arbitraries.integers().greaterOrEqual(0).sample();
                ((UInt4Vector) vector).setSafe(i, rand);
            }
        }
        vector.setValueCount(position + length);
        return vector.getValueCount();
    }
}
