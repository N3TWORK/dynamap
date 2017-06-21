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
    private final String dynamoName;
    private final String type;
    private final String defaultValue;
    private final MultiValue multiValue;
    private final Boolean useDefaultForNulls;
    private final Boolean replace;
    private final Boolean persisted;

    private boolean generatedType;

    @JsonCreator
    public Field(@JsonProperty("name") String name, @JsonProperty("dynamoName") String dynamoName, @JsonProperty("type") String type,
                 @JsonProperty("default") String defaultValue, @JsonProperty("multivalue") MultiValue multiValue,
                 @JsonProperty("useDefaultForNulls") Boolean useDefaultForNulls, @JsonProperty("replace") Boolean replace, @JsonProperty("persisted") Boolean persisted) {
        this.name = name;
        this.dynamoName = dynamoName;
        this.type = type;
        if (defaultValue == null) {
            if (type.equals("Map")) {
                this.defaultValue = "Collections.emptyMap()";
            } else if (type.equals("List")) {
                this.defaultValue = "Collections.emptyList()";
            } else if (type.equals("Set")) {
                this.defaultValue = "Collections.emptySet()";
            } else if (type.equals("Integer")) {
                this.defaultValue = "0";
            } else if (type.equals("Long")) {
                this.defaultValue = "0L";
            } else if (type.equals("Float") || type.equals("Double")) {
                this.defaultValue = "0.0";
            } else if (type.equals("Boolean")) {
                this.defaultValue = "Boolean.FALSE";
            } else if (type.equals("String")) {
                this.defaultValue = "\" \"";
            } else {
                this.defaultValue = "null";
            }
        } else if (type.equals("String")) {
            this.defaultValue = "\"" + defaultValue + "\"";
        } else {
            this.defaultValue = defaultValue;
        }
        this.multiValue = multiValue;
        this.useDefaultForNulls = useDefaultForNulls == null ? Boolean.FALSE : useDefaultForNulls;
        this.replace = replace == null ? Boolean.FALSE : replace;
        this.persisted = persisted == null ? Boolean.TRUE : persisted;
    }

    public String getName() {
        return name;
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

    public Boolean isUseDefaultForNulls() {
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


    public boolean isPersisted() {
        return persisted;
    }
}
