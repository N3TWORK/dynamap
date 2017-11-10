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

import com.fasterxml.jackson.annotation.*;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Field {

    public enum MultiValue {

        MAP("Map"), LIST("List"), SET("Set");

        private final String value;

        MultiValue(String value) {
            this.value = value;
        }

        @JsonValue
        public String getValue() {
            return value;
        }
    }

    private final String name;
    private final String description;
    private final String dynamoName;
    private final String type;
    private final String defaultValue;
    private final MultiValue multiValue;
    private final Boolean useDefaultForNulls;
    private final Boolean replace;
    private final Boolean persist;
    private final Boolean serialize;
    private final Boolean deltas;

    private boolean generatedType;

    @JsonCreator
    public Field(@JsonProperty("name") String name, @JsonProperty("description") String description, @JsonProperty("dynamoName") String dynamoName, @JsonProperty("type") String type,
                 @JsonProperty("default") String defaultValue, @JsonProperty("multivalue") MultiValue multiValue,
                 @JsonProperty("useDefaultForNulls") Boolean useDefaultForNulls, @JsonProperty("replace") Boolean replace, @JsonProperty("persist") Boolean persist, @JsonProperty("serialize") Boolean serialize, @JsonProperty("deltas") Boolean deltas) {
        this.name = name;
        this.description = description;
        this.dynamoName = dynamoName;
        this.type = type;
        this.useDefaultForNulls = useDefaultForNulls == null ? Boolean.FALSE : useDefaultForNulls;
        if (defaultValue == null) {
            if (this.useDefaultForNulls) {
                throw new RuntimeException("Invalid field definition for :" + name + ". Must specify a default value if useDefaultForNulls is set");
            }
            if (type.equals("Map")) {
                this.defaultValue = "Collections.emptyMap()";
            } else if (type.equals("List")) {
                this.defaultValue = "Collections.emptyList()";
            } else if (type.equals("Set")) {
                this.defaultValue = "Collections.emptySet()";
            } else {
                this.defaultValue = "null";
            }
        } else if (type.equals("String")) {
            this.defaultValue = "\"" + defaultValue + "\"";
        } else {
            this.defaultValue = defaultValue;
        }
        this.multiValue = multiValue;
        this.replace = replace == null ? Boolean.FALSE : replace;
        this.persist = persist == null ? Boolean.TRUE : persist;
        this.serialize = serialize == null ? Boolean.TRUE : serialize;
        this.deltas = deltas == null ? Boolean.TRUE : deltas;
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

    public boolean isNumber() {
        return type.equals("Number") || type.equals("Integer") || type.equals("Long") || type.equals("Float") || type.equals("Double");
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public MultiValue getMultiValue() {
        return multiValue;
    }

    public Boolean useDefaultForNulls() {
        return useDefaultForNulls;
    }

    public Boolean isReplace() {
        return replace;
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

}
