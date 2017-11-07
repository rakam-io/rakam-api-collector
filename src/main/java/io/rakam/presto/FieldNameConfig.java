/*
 * Licensed under the Rakam Incorporation
 */

package io.rakam.presto;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.configuration.Config;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class FieldNameConfig
{
    private String checkpointField = "_shard_time";
    private String userFieldName = "_actor";
    private String timeField = "_time";
    private Set<String> excludedColumns = new HashSet<>();

    private UserType userFieldType = UserType.STRING;

    @Config("database.checkpoint-field")
    public FieldNameConfig setCheckpointField(String checkpointField)
    {
        this.checkpointField = checkpointField;
        return this;
    }

    @Config("database.user-field-name")
    public FieldNameConfig setUserFieldName(String userFieldName)
    {
        this.userFieldName = userFieldName;
        return this;
    }

    @Config("database.user-field-type")
    public FieldNameConfig setUserFieldType(UserType userFieldType)
    {
        this.userFieldType = userFieldType;
        return this;
    }

    @Config("database.time-field-name")
    public FieldNameConfig setTimeField(String timeField)
    {
        this.timeField = timeField;
        return this;
    }

    @Config("database.user-excluded-columns")
    public FieldNameConfig setExcludedColumns(String excludedColumns)
    {
        if (excludedColumns != null) {
            excludedColumns = excludedColumns.replaceAll("\\s+", "");
            this.excludedColumns = new HashSet<>(Arrays.asList(excludedColumns.split(",")));
        }
        return this;
    }

    @Config("database.whitelisted.collections")
    public FieldNameConfig setWhitelistedCollections(String collections)
    {
        if (collections != null) {
            collections = collections.replaceAll("\\s+", "");
            this.whitelistedCollections = new HashSet<>(Arrays.asList(collections.split(",")));
        }
        return this;
    }

    public String getCheckpointField()
    {
        return checkpointField;
    }

    public String getUserFieldName()
    {
        return userFieldName;
    }

    public UserType getUserFieldType()
    {
        return userFieldType;
    }

    public String getTimeField()
    {
        return timeField;
    }

    public Set<String> getExcludedColumns()
    {
        return excludedColumns;
    }

    public Set<String> getWhitelistedCollections() {return whitelistedCollections;}

    public enum UserType
    {
        DOUBLE(DoubleType.DOUBLE),
        INTEGER(IntegerType.INTEGER),
        LONG(BigintType.BIGINT),
        STRING(VarcharType.VARCHAR);

        private final Type type;

        UserType(Type type)
        {
            this.type = type;
        }

        public Type getType()
        {
            return type;
        }
    }
}