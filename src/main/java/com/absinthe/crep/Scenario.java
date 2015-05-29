package com.absinthe.crep;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by lalith on 07.03.15.
 */
public class Scenario {

    public static Random random = new Random();
    public static String [] columnNames;
    public static final ReentrantLock lock = new ReentrantLock();
    public static final Condition notCongested = lock.newCondition();
    private static boolean canProceed = true;
    private static int throughput = 20000;
    private static long lastCount = 0;

    public static void setColumnNames(String schemaFile) {
        if (schemaFile == null) {
            return;
        }
        try {
            InputStream stream = new FileInputStream(schemaFile);

            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            String line = reader.readLine();
            assert line != null;
            columnNames = line.split(" ");
            assert columnNames.length > 0;
        } catch (FileNotFoundException e) {
            throw new AssertionError(e);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static long executeFromFile(String filename, Scheduler sched, Conf conf) throws InterruptedException {
        try {
            InputStream stream = new FileInputStream(filename);

            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

            String line = null;
            int totalOps = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                String [] splits = line.split(" ");
                if (splits[0].equals("R")) {
                    // Each line looks as follows
                    // : "R k1 k2 k3 k4..."
                    //
                    // Each line is a single multi-key request. Whether or not
                    // a multi-key request is sent out as a single multi-read
                    // or multiple reads is realized by the disable_client_multireads
                    // flag in the configuration file.

                    if (conf.disable_client_multireads == false) {
                        // We collapse a batch request into a single read.
                        // The fanout is pushed to the Cassandra coordinator.
                        List<String> keys = new ArrayList<>();
                        for (int i = 1; i < splits.length; i++) {
                            keys.add(splits[i]);
                        }
                        ReadRequest req = new ReadRequest(conf.column_family_name,
                                keys, null);
                        sched.schedule(req);
                        totalOps++;
                    } else {
                        // The request fanout happens at the client.
                        for (int i = 1; i < splits.length; i++) {
                            List<String> keys = new ArrayList<>();
                            keys.add(splits[i]);
                            ReadRequest req = new ReadRequest(conf.column_family_name,
                                    keys, null);
                            sched.schedule(req);
                            totalOps++;
                        }
                    }
                } else if (splits[0].equals("I")) {
                    assert columnNames.length > 0;
                    Map<String, Map<String, Integer>> mutations = new HashMap<>();
                    Map<String, Integer> columns = new HashMap<>();

                    // The index starts from 2 onwards, because
                    // at i = 0, we have "I", and at i = 1, we
                    // have the key name.
                    int j = 0;
                    for (int i = 2; i < splits.length; i += 1) {
                        columns.put(columnNames[j], validToken(splits[i]));
                        j += 1;
                    }
                    String key = splits[1];
                    mutations.put(key, columns);
                    InsertRequest req = new InsertRequest(conf.column_family_name,
                                                          mutations);
                    sched.schedule(req);
                    totalOps ++;
                }

                try{
                    lock.lock();

                    while (totalOps >= lastCount + conf.workload_gen_throttle * throughput) {
                        notCongested.await();
                    }
                } finally {
                    lock.unlock();
                }

            }

            return totalOps;
        } catch (FileNotFoundException e) {
            throw new AssertionError(e);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static long syntheticLoad(Scheduler sched, Conf conf) throws InterruptedException {
        long startRecord = conf.record_start;
        long numRecords = conf.num_records;
        Random random = new Random();
        long totalOps = 0;

        for (long keyId = startRecord; keyId < startRecord + numRecords; keyId++) {
            Map<String, Map<String, Integer>> mutations = new HashMap<>();
            Map<String, Integer> columns = new HashMap<>();

            // The index starts from 2 onwards, because
            // at i = 0, we have "I", and at i = 1, we
            // have the key name.
            for (int i = 1; i < columnNames.length; i += 1) {
                columns.put(columnNames[i], Math.abs(random.nextInt()));
            }
            String keyString = String.valueOf(keyId);
            mutations.put(keyString, columns);
            InsertRequest req = new InsertRequest(conf.column_family_name,
                    mutations);
            sched.schedule(req);

            totalOps += 1;

            try{
                lock.lock();

                while (totalOps >= lastCount + conf.workload_gen_throttle * throughput) {
                    notCongested.await();
                }
            } finally {
                lock.unlock();
            }
        }

        return numRecords;
    }

    public static Integer validToken(String token) {
        if (token.equals("NULL")) {
            return 0;
        }
        return Integer.parseInt(token);
    }

    public static void canProceed(boolean canProceed, long completedOps, double throughput) {
        // Note, throughput here is expressed across the sampling interval, which
        // depends on the StatusThread's loop interval.
        try {
            lock.lock();
            if (completedOps - lastCount > throughput / 2.0) {
                notCongested.signal();
                lastCount = completedOps;
                throughput = (long) throughput;
            }
        } finally {
            lock.unlock();
        }
    }
}
