/*
 * Licensed under the Rakam Incorporation
 */

package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.type.Type;

public final class Types
{
    private Types() {}

    public static Type getElementType(Type type)
    {
        return type.getTypeParameters().get(0);
    }

    public static Type getKeyType(Type type)
    {
        return type.getTypeParameters().get(0);
    }

    public static Type getValueType(Type type)
    {
        return type.getTypeParameters().get(1);
    }
}

