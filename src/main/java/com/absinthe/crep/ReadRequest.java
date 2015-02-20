package com.absinthe.crep;

import java.util.Set;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class ReadRequest extends Request {

    public final String table;
    public final String key;
    public final Set<String> fields;

    public ReadRequest(String table, String key, Set<String> fields) {
        this.table = table;
        this.key = key;
        this.fields = fields;
    }
}
