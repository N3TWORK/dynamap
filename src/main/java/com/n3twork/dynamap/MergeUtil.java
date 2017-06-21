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

import com.google.common.collect.Maps;

import java.util.*;

public class MergeUtil {

    public static <K, V> Map<K, V> mergeUpdatesAndDeletes(Map<K, V> orig, Map<K, V> updates, Set<K> deleted, boolean clear) {
        if (clear) {
            return Collections.emptyMap();
        }
        if (updates == null && deleted == null) {
            return orig;
        }
        Map<K, V> merged = Maps.newHashMap(orig);
        if (updates != null) {
            merged.putAll(updates);
        }
        if (deleted != null) {
            for (K key : deleted) {
                merged.remove(key);
            }
        }
        return merged;
    }

    public static <K> Set<K> mergeUpdatesAndDeletes(Set<K> orig, Set<K> deltas, Set<K> setUpdates, Collection<K> deletedKeys, boolean clear) {
        if (clear) {
            return Collections.emptySet();
        }
        Set<K> keys = new HashSet<>();
        if (orig != null) {
            keys.addAll(orig);
        }
        if (deltas != null) {
            keys.addAll(deltas);
        }
        if (setUpdates != null) {
            keys.addAll(setUpdates);
        }
        if (deletedKeys != null) {
            keys.removeAll(deletedKeys);
        }
        return keys;
    }

    public static <V> List<V> mergeAdds(List<V> orig, List<V> adds, boolean clear) {
        if (clear) {
            return Collections.emptyList();
        }
        List<V> values = new ArrayList();
        if (orig != null) {
            values.addAll(orig);
        }
        if (adds != null) {
            values.addAll(adds);
        }
        return values;
    }

    public static <V extends Number> V getLatestNumericValue(V orig, V delta, V update) {
        if (update != null) {
            return update;
        }
        if (delta != null) {
            if (orig instanceof Integer) {
                return (V) Integer.valueOf(orig.intValue() + delta.intValue());
            } else if (orig instanceof Long) {
                return (V) Long.valueOf(orig.longValue() + delta.longValue());
            } else if (orig instanceof Float) {
                return (V) Float.valueOf(orig.floatValue() + delta.floatValue());
            } else if (orig instanceof Double) {
                return (V) Double.valueOf(orig.doubleValue() + delta.doubleValue());
            } else {
                throw new RuntimeException("Unsupported type" + orig.getClass().getName());
            }
        }
        return orig;
    }

    public static <K, V extends Number> V getLatestNumericValue(K key, V orig, Map<K, V> deltas, Map<K, V> setUpdates, boolean clear) {
        if (clear) {
            return null;
        }
        if (setUpdates != null) {
            if (setUpdates.get(key) != null) {
                return setUpdates.get(key);
            }
        }
        if (deltas != null) {
            if (deltas.get(key) != null) {
                if (orig instanceof Integer) {
                    return (V) Integer.valueOf(orig.intValue() + deltas.get(key).intValue());
                } else if (orig instanceof Long) {
                    return (V) Long.valueOf(orig.longValue() + deltas.get(key).longValue());
                } else if (orig instanceof Float) {
                    return (V) Float.valueOf(orig.floatValue() + deltas.get(key).floatValue());
                } else if (orig instanceof Double) {
                    return (V) Double.valueOf(orig.doubleValue() + deltas.get(key).doubleValue());
                } else {
                    throw new RuntimeException("Unsupported type" + orig.getClass().getName());
                }
            }
        }
        return orig;
    }

    public static <K, V> V getLatestValue(K key, V orig, Map<K, V> updates, Set<K> deletes, boolean clear) {
        if (clear) {
            return null;
        }
        if (deletes != null && deletes.contains(key)) {
            return null;
        }
        if (updates != null && updates.containsKey(key)) {
            return updates.get(key);
        }
        return orig;
    }


}
