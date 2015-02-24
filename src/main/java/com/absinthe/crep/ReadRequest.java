package com.absinthe.crep;

import java.util.List;
import java.util.Set;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class ReadRequest extends Request {

    public final String table;
    public final List<String> keys;
    public final Set<String> fields;

    public ReadRequest(String table, List<String> keys, Set<String> fields) {
        this.table = table;
        this.keys = keys;
        this.fields = fields;
    }
}
