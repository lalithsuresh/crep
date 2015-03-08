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

    public final String cluster_name;
    public final String keyspace_name;
    public final String column_family_name;
    public final int max_conns;
    public final String hosts;
    public final int async_executor_num_threads;
    public final int num_client_threads;
    public final int total_operations;
    public final int status_thread_update_interval_ms;
    public final boolean debug;
    public final String workload_file;

    private Conf(String cluster_name,
                 String keyspace_name,
                 String column_family_name,
                 int max_conns,
                 String hosts,
                 int async_executor_num_threads,
                 int num_client_threads,
                 int total_operations,
                 int status_thread_update_interval_ms,
                 boolean debug,
                 String workload_file) {
      this.cluster_name = cluster_name;
      this.keyspace_name = keyspace_name;
      this.column_family_name = column_family_name;
      this.max_conns = max_conns;
      this.async_executor_num_threads = async_executor_num_threads;
      this.hosts = hosts;
      this.num_client_threads = num_client_threads;
      this.total_operations = total_operations;
      this.status_thread_update_interval_ms = status_thread_update_interval_ms;
      this.debug = debug;
      this.workload_file = workload_file;
    }

    public static Conf getConf(String confFilePath) {
        try {
            Yaml yaml = new Yaml();
            InputStream stream = new FileInputStream(confFilePath);
            Map conf_map = (Map) yaml.load(stream);
            System.out.println(conf_map);

            return new Conf ((String) conf_map.get("cluster_name"),
                            (String) conf_map.get("keyspace_name"),
                            (String) conf_map.get("column_family_name"),
                            (int) conf_map.get("max_conns"),
                            (String) conf_map.get("hosts"),
                            (int) conf_map.get("async_executor_num_threads"),
                            (int) conf_map.get("num_client_threads"),
                            (int) conf_map.get("total_operations"),
                            (int) conf_map.get("status_thread_update_interval_ms"),
                            (boolean) conf_map.get("debug"),
                            (String) conf_map.get("workload_file"));
        } catch (FileNotFoundException e) {
            throw new AssertionError("Conf file not found");
        }
    }

}
