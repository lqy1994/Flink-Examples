package com.meituan.meishi.data.lqy.flink.examples.utils;

import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.leadpony.justify.api.InstanceType;
import org.leadpony.justify.api.JsonSchemaBuilder;
import org.leadpony.justify.api.JsonSchemaBuilderFactory;
import org.leadpony.justify.api.JsonValidationService;

import java.util.ArrayList;
import java.util.List;

public class RichRowTypeInfo extends RowTypeInfo {
    private static JsonValidationService service = JsonValidationService.newInstance();
    private static JsonSchemaBuilderFactory f = service.createSchemaBuilderFactory();
    String[] comments;

    RichRowTypeInfo(TypeInformation<?>[] types, String[] fieldNames, String[] Comments) {
        super(types, fieldNames);
        this.comments = Comments;

    }

    public static Builder bulider() {
        return new Builder();
    }

    public String[] getComments() {
        return comments;
    }

    public void setComments(String[] comments) {
        this.comments = comments;
    }

    public String toJsonSchema() {
        JsonSchemaBuilder jb = f.createBuilder();
        jb.withType(InstanceType.OBJECT);
        jb.withRequired(fieldNames);
        for (int i = 0; i < this.fieldNames.length; i++) {
            jb.withProperty(fieldNames[i], convertType(types[i]).withDescription(this.comments[i]).build());
        }
        return jb.build().toString();
    }

    JsonSchemaBuilder convertObject(RichRowTypeInfo rt) {
        JsonSchemaBuilder jb = f.createBuilder();
        jb.withType(InstanceType.OBJECT);
        jb.withRequired(rt.fieldNames);
        for (int i = 0; i < rt.fieldNames.length; i++) {
            jb.withProperty(rt.fieldNames[i], convertType(rt.types[i]).withDescription(rt.comments[i]).build());
        }


        return jb;
    }

    JsonSchemaBuilder convertObjectArray(TypeInformation<?> info) {
        JsonSchemaBuilder jb = f.createBuilder();
        jb.withType(InstanceType.ARRAY).withItems(convertType(info).build());
        return jb;

    }

    private JsonSchemaBuilder convertType(TypeInformation info) {
        JsonSchemaBuilder jb = f.createBuilder();

        if (info == BasicTypeInfo.VOID_TYPE_INFO) {
            return jb.withType(InstanceType.NULL);
        } else if (info == Types.BOOLEAN) {
            return jb.withType(InstanceType.BOOLEAN);
        } else if (info == Types.STRING) {
            return jb.withType(InstanceType.STRING);
        } else if (info == Types.BIG_DEC) {

            return jb.withType(InstanceType.NUMBER);
        } else if (info == Types.LONG) {
            {
                return jb.withType(InstanceType.INTEGER);
            }
        } else if (info == Types.SQL_TIME) {
            return jb.withType(InstanceType.STRING);
        } else if (info == Types.SQL_TIMESTAMP) {
            return jb.withType(InstanceType.STRING);
        } else if (info instanceof RichRowTypeInfo) {
            return convertObject((RichRowTypeInfo) info);
        } else if (info instanceof ObjectArrayTypeInfo) {
            return convertObjectArray(((ObjectArrayTypeInfo) info).getComponentInfo());
        } else if (info instanceof BasicArrayTypeInfo) {
            return convertObjectArray(((BasicArrayTypeInfo) info).getComponentInfo());
        } else if (info instanceof PrimitiveArrayTypeInfo && ((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
            return convertObjectArray(((PrimitiveArrayTypeInfo) info).getComponentType());
        }
        return jb;
    }

    public static class Builder {
        List<String> fields = new ArrayList<>();
        List<String> comments = new ArrayList<>();
        List<TypeInformation> types = new ArrayList<>();

        public Builder field(String field, TypeInformation type, String comment) {
            fields.add(field);
            types.add(type);
            comments.add(comment);
            return this;
        }

        public RichRowTypeInfo build() {

            return new RichRowTypeInfo(types.toArray(new TypeInformation[types.size()]), fields.toArray(new String[fields.size()]), comments.toArray(new String[comments.size()]));

        }
    }

}