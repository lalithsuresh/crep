package com.absinthe.crep;

import org.apache.avro.generic.GenericData;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by lalith on 07.03.15.
 */
public class Scenario {

    public static Random random = new Random();

    public static void execute(String filename, Scheduler sched, Conf conf) {
        try {
            InputStream stream = new FileInputStream(filename);

            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

            String line = null;

            while ((line = reader.readLine()) != null) {
                line = line.trim();
                String [] splits = line.split(" ");
                List<String> keys = new ArrayList<>();
                for (int i = 1; i < splits.length; i++) {
                    keys.add(splits[i]);
                }
                if (splits[0].equals("R")) {
                    ReadRequest req = new ReadRequest(conf.column_family_name,
                                                      keys, null);
                    sched.schedule(req);
                }
            }
        } catch (FileNotFoundException e) {
            throw new AssertionError(e);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
