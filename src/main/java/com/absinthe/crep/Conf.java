package com.absinthe.crep;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lalith on 24.02.15.
 */
public class Conf {
    Yaml yaml = new Yaml();
    Map conf_map = new HashMap<>();

    public String cluster_name;
    public String keyspace_name;
    public String column_family_name;
    public int max_conns;
    public String hosts;
    public int async_executor_num_threads;

    public Conf(String confFilePath) {
        try {
            InputStream stream = new FileInputStream(confFilePath);
            conf_map = (Map) yaml.load(stream);
            System.out.println(conf_map);

            this.cluster_name = (String) conf_map.get("cluster_name");
            this.keyspace_name= (String) conf_map.get("keyspace_name");
            this.column_family_name = (String) conf_map.get("column_family_name");
            this.max_conns = (int) conf_map.get("max_conns");
            this.async_executor_num_threads = (int) conf_map.get("async_executor_num_threads");
            this.hosts = (String) conf_map.get("hosts");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
    }

}
