package com.absinthe.crep;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Created by lalith on 09.03.15.
 */
public abstract class ClientDriver {
    protected volatile int totalCompletedOps = 0;

    abstract public void init(Conf conf);
    abstract public void shutDown();

    abstract public void read(ReadRequest req);

    abstract public void insert(InsertRequest req);

    public int getTotalCompletedOps() {
        return totalCompletedOps;
    }
}
