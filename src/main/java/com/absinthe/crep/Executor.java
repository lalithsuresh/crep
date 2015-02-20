package com.absinthe.crep;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.transport.TTransport;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by lalithsuresh on 2/18/15.
 */
public class Executor implements Runnable {

    private final Queue<Request> taskQueue = new ConcurrentLinkedQueue<Request>();
    private volatile boolean terminate = false;
    private String hostname;
    private int port;


    public static final String CONNECTION_RETRY_PROPERTY = "cassandra.connectionretries";
    public static final String CONNECTION_RETRY_PROPERTY_DEFAULT = "300";

    public static final String OPERATION_RETRY_PROPERTY = "cassandra.operationretries";
    public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";

    public static final String USERNAME_PROPERTY = "cassandra.username";
    public static final String PASSWORD_PROPERTY = "cassandra.password";

    public static final String COLUMN_FAMILY_PROPERTY = "cassandra.columnfamily";
    public static final String COLUMN_FAMILY_PROPERTY_DEFAULT = "data";

    public static final String READ_CONSISTENCY_LEVEL_PROPERTY = "cassandra.readconsistencylevel";
    public static final String READ_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.writeconsistencylevel";
    public static final String WRITE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY = "cassandra.scanconsistencylevel";
    public static final String SCAN_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";

    public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY = "cassandra.deleteconsistencylevel";
    public static final String DELETE_CONSISTENCY_LEVEL_PROPERTY_DEFAULT = "ONE";


    TTransport tr;
    Cassandra.Client client;

    ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;


    Executor () {

    }

    public void init() {

    }


    @Override
    public void run() {

        while (terminate == false) {
            Request req = taskQueue.poll();

            // XXX: issue calls to Cassandra here.
        }
    }

    public void enqueue(Request req) {
        taskQueue.add(req);
    }
}
