/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.raptor.storage.Types.getElementType;
import static com.facebook.presto.raptor.storage.Types.getKeyType;
import static com.facebook.presto.raptor.storage.Types.getValueType;
import static com.facebook.presto.raptor.util.Types.isArrayType;
import static com.facebook.presto.raptor.util.Types.isMapType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.util.Objects.requireNonNull;

public class StorageTypeConverter
{
    private final TypeManager typeManager;

    public StorageTypeConverter(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Type toStorageType(Type type)
    {
        if (type.equals(BOOLEAN) ||
                type.equals(BIGINT) ||
                type.equals(INTEGER) ||
                type.equals(SMALLINT) ||
                type.equals(TINYINT) ||
                type.equals(DOUBLE) ||
                type.equals(REAL) ||
                isCharType(type) ||
                isVarcharType(type) ||
                type.equals(VARBINARY)) {
            return type;
        }
        if (type.equals(DATE) || type.equals(TIME)) {
            return INTEGER;
        }
        if (type.equals(TIMESTAMP)) {
            return BIGINT;
        }
        if (isArrayType(type)) {
            return new ArrayType(toStorageType(getElementType(type)));
        }
        if (isMapType(type)) {
            return mapType(toStorageType(getKeyType(type)), toStorageType(getValueType(type)));
        }
        throw new PrestoException(NOT_SUPPORTED, "Type not supported: " + type);
    }

    private Type mapType(Type keyType, Type valueType)
    {
        return typeManager.getParameterizedType(StandardTypes.MAP, ImmutableList.of(
                TypeSignatureParameter.of(keyType.getTypeSignature()),
                TypeSignatureParameter.of(valueType.getTypeSignature())));
    }
}