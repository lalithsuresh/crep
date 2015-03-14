package com.absinthe.crep;

import org.apache.avro.generic.GenericData;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

/**
 * Created by lalith on 07.03.15.
 */
public class Scenario {

    public static Random random = new Random();
    public static String [] columnNames;

    public static void setColumnNames(String schemaFile) {
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

    public static long executeFromFile(String filename, Scheduler sched, Conf conf) {
        try {
            InputStream stream = new FileInputStream(filename);

            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

            String line = null;
            int totalOps = 0;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                String [] splits = line.split(" ");
                if (splits[0].equals("R")) {
                    List<String> keys = new ArrayList<>();
                    for (int i = 1; i < splits.length; i++) {
                        keys.add(splits[i]);
                    }
                    ReadRequest req = new ReadRequest(conf.column_family_name,
                                                      keys, null);
                    sched.schedule(req);
                    totalOps ++;
                } else if (splits[0].equals("I")) {
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
            }

            return totalOps;
        } catch (FileNotFoundException e) {
            throw new AssertionError(e);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public static long executeSynthentic(Scheduler sched, Conf conf) {
        long numRecords = conf.num_records;
        Random random = new Random();

        for (int keyId = 0; keyId < numRecords; keyId++) {
            Map<String, Map<String, Integer>> mutations = new HashMap<>();
            Map<String, Integer> columns = new HashMap<>();

            // The index starts from 2 onwards, because
            // at i = 0, we have "I", and at i = 1, we
            // have the key name.
            for (int i = 0; i < columnNames.length; i += 1) {
                columns.put(columnNames[0], random.nextInt());
            }
            String keyString = String.valueOf(keyId);
            mutations.put(keyString, columns);
            InsertRequest req = new InsertRequest(conf.column_family_name,
                    mutations);
            sched.schedule(req);
        }

        return numRecords;
    }

    public static Integer validToken(String token) {
        if (token.equals("NULL")) {
            return 0;
        }
        return Integer.parseInt(token);
    }
}
