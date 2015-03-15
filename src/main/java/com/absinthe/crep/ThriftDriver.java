package com.absinthe.crep;

import org.apache.cassandra.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by lalith on 15.03.15.
 */
public class ThriftDriver extends ClientDriver  {

    static Random random = new Random();
    public static final int Ok = 0;
    public static final int Error = -1;

    public int ConnectionRetries;
    public int OperationRetries;
    public String column_family;

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

    ColumnParent parent;

    ConsistencyLevel readConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel writeConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel scanConsistencyLevel = ConsistencyLevel.ONE;
    ConsistencyLevel deleteConsistencyLevel = ConsistencyLevel.ONE;
    Cassandra.Client client;

    TTransport tr;
    ByteBuffer emptyByteBuffer = ByteBuffer.wrap(new byte[0]);
    List<Mutation> mutations = new ArrayList<Mutation>();
    Map<String, List<Mutation>> mutationMap = new HashMap<String, List<Mutation>>();
    Map<ByteBuffer, Map<String, List<Mutation>>> record = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();


    @Override
    public void init(Conf conf) {
        String hosts = conf.hosts;
        assert hosts != null;

        column_family = conf.column_family_name;
        parent = new ColumnParent(column_family);

        ConnectionRetries = 1;
        OperationRetries = 1;

        String[] allhosts = hosts.split(",");
        String myhost = allhosts[random.nextInt(allhosts.length)];


        for (int retry = 0; retry < ConnectionRetries; retry++)
        {
            String hostname = myhost.split(":")[0];
            int port = Integer.valueOf(myhost.split(":")[1]);
            tr = new TFramedTransport(new TSocket(hostname, port));
            TProtocol proto = new TBinaryProtocol(tr);
            client = new Cassandra.Client(proto);
            try
            {
                tr.open();
                break;
            } catch (Exception e)
            {
                e.printStackTrace();
            }
            try
            {
                Thread.sleep(1000);
            } catch (InterruptedException e)
            {
            }
        }
        System.out.println("WHAT" + tr.isOpen());
        try {
            client.set_keyspace(conf.keyspace_name);
        } catch (InvalidRequestException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutDown() {
        tr.close();
    }

    @Override
    public void read(ReadRequest req) {
        try {
            SlicePredicate predicate;
            if (req.fields == null) {
                predicate = new SlicePredicate().setSlice_range(new SliceRange(emptyByteBuffer, emptyByteBuffer, false, 1000000));

            } else {
                ArrayList<ByteBuffer> fieldlist = new ArrayList<ByteBuffer>(req.fields.size());
                for (String s : req.fields) {
                    fieldlist.add(ByteBuffer.wrap(s.getBytes("UTF-8")));
                }

                predicate = new SlicePredicate().setColumn_names(fieldlist);
            }

            List<ByteBuffer> keys =  new ArrayList<>();
            for (String key: req.keys) {
                keys.add(ByteBuffer.wrap(key.getBytes("UTF-8")));
            }

            Map<ByteBuffer,List<ColumnOrSuperColumn>> results = client.multiget_slice(keys,
                    parent, predicate, readConsistencyLevel);
            if (results != null) {
                System.out.println("Read: " + results.size());
            }

        } catch (UnavailableException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        } catch (InvalidRequestException e) {
            e.printStackTrace();
        } catch (TimedOutException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void insert(InsertRequest req) {
        try
        {
            for (Map.Entry<String, Map<String, Integer>> entry: req.mutations.entrySet()) {

                ByteBuffer wrappedKey = ByteBuffer.wrap(entry.getKey().getBytes("UTF-8"));

                Column col;
                ColumnOrSuperColumn column;
                for (Map.Entry<String, Integer> columnEntry : entry.getValue().entrySet())
                {
                    col = new Column();
                    col.setName(ByteBuffer.wrap(columnEntry.getKey().getBytes("UTF-8")));
                    ByteBuffer b = ByteBuffer.allocate(4);
                    b.putInt(columnEntry.getValue());
                    col.setValue(b);
                    col.setTimestamp(System.currentTimeMillis());

                    column = new ColumnOrSuperColumn();
                    column.setColumn(col);

                    mutations.add(new Mutation().setColumn_or_supercolumn(column));
                }

                mutationMap.put(column_family, mutations);
                record.put(wrappedKey, mutationMap);

            }
            client.batch_mutate(record, writeConsistencyLevel);

            mutations.clear();
            mutationMap.clear();
            record.clear();

            totalCompletedOps++;
            return;
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
