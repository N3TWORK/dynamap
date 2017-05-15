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

public class Alias {

    private String prefix;
    private int counter;

    public Alias(String prefix) {
        this.prefix = prefix;

    }

    public String next() {
        return prefix + counter++;
    }

    public static void main(String[] args) {
        Alias a = new Alias("#myvar");
        for (int i = 0; i < 100; i++) {
            System.out.println(a.next());
        }
    }
}
