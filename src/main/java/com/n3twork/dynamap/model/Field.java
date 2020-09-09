/*
    Copyright 2017 N3TWORK INC

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package com.n3twork.dynamap.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Field {

    private final String name;
    private final String description;
    private final String dynamoName;
    private final String defaultValue;
    private final String type;
    private final String elementType;
    private final Boolean useDefaultForNulls;
    private final Boolean replace;
    private final Boolean persist;
    private final Boolean serialize;
    private final Boolean deltas;
    private final String serializeAsListElementId;
    private final String compressCollection;
    private final boolean isCollection;
    private final boolean isTtl;

    private boolean generatedType;

    @JsonCreator
    public Field(@JsonProperty("name") String name, @JsonProperty("description") String description, @JsonProperty("dynamoName") String dynamoName,
                 @JsonProperty("type") String type, @JsonProperty("elementType") String elementType, @JsonProperty("default") String defaultValue,
                 @JsonProperty("useDefaultForNulls") Boolean useDefaultForNulls, @JsonProperty("replace") Boolean replace,
                 @JsonProperty("persist") Boolean persist, @JsonProperty("serialize") Boolean serialize, @JsonProperty("deltas") Boolean deltas,
                 @JsonProperty("serializeAsListElementId") String serializeAsListElementId, @JsonProperty("compressCollection") String compressCollection) {
        if ("ttl".equals(type)) {
            if (null != persist && !persist) {
                throw new IllegalArgumentException("Invalid field definition for " + name + ". TTL field must be persisted.");
            }
            this.isTtl = true;
            type = "Long";
        } else {
            this.isTtl = false;
        }
        this.name = name;
        this.description = description;
        this.dynamoName = dynamoName;
        this.type = type;
        this.elementType = elementType;
        this.useDefaultForNulls = useDefaultForNulls == null ? Boolean.FALSE : useDefaultForNulls;

        isCollection = ((type.equals("Map") || type.equals("List") || type.equals("Set"))) ? true : false;

        if (defaultValue == null && (useDefaultForNulls != null && useDefaultForNulls)) {
            throw new RuntimeException("Invalid field definition for :" + name + ". Must specify a default value if useDefaultForNulls is set.");
        }

        this.defaultValue = defaultValue;

        this.replace = replace == null ? Boolean.FALSE : replace;
        this.persist = persist == null ? Boolean.TRUE : persist;
        this.serialize = serialize == null ? Boolean.TRUE : serialize;
        this.deltas = deltas == null ? Boolean.TRUE : deltas;
        this.serializeAsListElementId = serializeAsListElementId;
        if (compressCollection != null && !isCollection) {
            throw new IllegalArgumentException("Cannot use compressCollection with scalar fields");
        }
        this.compressCollection = compressCollection;
    }


    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getDynamoName() {
        return dynamoName;
    }

    public String getType() {
        return type;
    }

    public String getElementType() {
        if (!isCollection) {
            return type;
        }
        return elementType;
    }

    public boolean isCollection() {
        return isCollection;
    }

    public boolean isNumber() {
        String elementType = getElementType();
        return elementType.equals("Number") || elementType.equals("Integer") || elementType.equals("Long") || elementType.equals("Float") || elementType.equals("Double");
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public Boolean useDefaultForNulls() {
        return useDefaultForNulls;
    }

    public Boolean isReplace() {
        return replace;
    }

    public String getSerializeAsListElementId() {
        return serializeAsListElementId;
    }

    public String getCompressCollection() {
        return compressCollection;
    }

    @JsonIgnore
    public boolean isCompressCollection() {
        return compressCollection != null;
    }

    @JsonIgnore
    public boolean isSerializeAsList() {
        return serializeAsListElementId != null;
    }

    @JsonIgnore
    public boolean isGeneratedType() {
        return generatedType;
    }

    public void setGeneratedType(boolean isGeneratedType) {
        this.generatedType = isGeneratedType;
    }


    public boolean isPersist() {
        return persist;
    }

    public Boolean isSerialize() {
        return persist || serialize;
    }

    public boolean useDeltas() {
        return deltas;
    }

    public boolean isTtl() {
        return isTtl;
    }
}
