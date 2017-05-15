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

package com.n3twork.dynamap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CustomType {

    public enum CustomTypeEnum {
        VALUE_A, VALUE_B;
    }

    private final String name;
    private final String value;
    private final CustomTypeEnum customTypeEnum;

    @JsonCreator
    public CustomType(@JsonProperty("name") String name, @JsonProperty("value") String value, @JsonProperty("customTypeEnum") CustomTypeEnum customTypeEnum) {
        this.name = name;
        this.value = value;
        this.customTypeEnum = customTypeEnum;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public CustomTypeEnum getCustomTypeEnum() {
        return customTypeEnum;
    }
}
