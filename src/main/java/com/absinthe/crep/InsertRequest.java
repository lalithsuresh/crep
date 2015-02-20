package com.absinthe.crep;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class InsertRequest {
    public final String table;
    public final String key;
    public final HashMap<String, String> values;

    public InsertRequest(String table, String key, HashMap<String, String> values) {
        this.table = table;
        this.key = key;
        this.values = values;
    }
}
