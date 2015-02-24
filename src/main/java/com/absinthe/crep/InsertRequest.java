package com.absinthe.crep;

import java.util.Map;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class InsertRequest extends Request {
    public final String table;
    public final Map<String, Map<String, Integer>> mutations;

    public InsertRequest(String table, Map<String, Map<String, Integer>> mutations) {
        this.table = table;
        this.mutations = mutations;
    }
}
