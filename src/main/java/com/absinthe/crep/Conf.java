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
    enum WorkloadType {
        FILE,
        SYNTHETIC
    }

    public final String cluster_name;
    public final String keyspace_name;
    public final String column_family_name;
    public final int max_conns;
    public final String hosts;
    public final int async_executor_num_threads;
    public final int num_client_threads;
    public final int status_thread_update_interval_ms;
    public final boolean debug;
    public final WorkloadType workload_type;
    public final String workload_file;
    public final String schema_file;
    public final long num_records;
    public final int scenario_threshold;

    private Conf(String cluster_name,
                 String keyspace_name,
                 String column_family_name,
                 int max_conns,
                 String hosts,
                 int async_executor_num_threads,
                 int num_client_threads,
                 int status_thread_update_interval_ms,
                 boolean debug,
                 WorkloadType workload_type,
                 String workload_file,
                 String schema_file,
                 long num_records,
                 int scenario_threshold) {
        this.cluster_name = cluster_name;
        this.keyspace_name = keyspace_name;
        this.column_family_name = column_family_name;
        this.max_conns = max_conns;
        this.async_executor_num_threads = async_executor_num_threads;
        this.hosts = hosts;
        this.num_client_threads = num_client_threads;
        this.status_thread_update_interval_ms = status_thread_update_interval_ms;
        this.debug = debug;
        this.workload_type = workload_type;
        this.workload_file = workload_file;
        this.schema_file = schema_file;
        this.num_records = num_records;
        this.scenario_threshold = scenario_threshold;
    }

    public static Conf getConf(String confFilePath) {
        try {
            Yaml yaml = new Yaml();
            InputStream stream = new FileInputStream(confFilePath);
            Map conf_map = (Map) yaml.load(stream);
            System.out.println(conf_map);

            String cluster_name = (String) conf_map.get("cluster_name");
            assert cluster_name != null;
            assert !cluster_name.isEmpty();

            String keyspace_name = (String) conf_map.get("keyspace_name");
            assert keyspace_name != null;
            assert !keyspace_name.isEmpty();

            String column_family_name = (String) conf_map.get("column_family_name");
            assert column_family_name != null;
            assert !column_family_name .isEmpty();

            int max_conns = (int) conf_map.get("max_conns");
            assert max_conns > 0;

            String hosts = (String) conf_map.get("hosts");
            assert hosts != null;
            assert !hosts.isEmpty();

            int async_executor_num_threads = (int) conf_map.get("async_executor_num_threads");
            assert async_executor_num_threads > 0;

            int num_client_threads = (int) conf_map.get("num_client_threads");
            assert num_client_threads > 0;

            int status_thread_update_interval_ms = (int) conf_map.get("status_thread_update_interval_ms");
            assert status_thread_update_interval_ms > 0;

            boolean debug = (boolean) conf_map.get("debug");

            WorkloadType workload_type = WorkloadType.valueOf((String) conf_map.get("workload_type"));

            String workload_file = (String) conf_map.get("workload_file");
            assert workload_file != null;
            assert !workload_file.isEmpty();

            String schema_file = (String) conf_map.get("schema_file");
            assert schema_file != null;
            assert !schema_file.isEmpty();

            long num_records = Long.valueOf(String.valueOf(conf_map.get("num_records")));
            assert num_records > 0;

            int scenario_threshold = (int) conf_map.get("scenario_threshold");
            assert scenario_threshold > 0;

            return new Conf (cluster_name,
                             keyspace_name,
                             column_family_name,
                             max_conns,
                             hosts,
                             async_executor_num_threads,
                             num_client_threads,
                             status_thread_update_interval_ms,
                             debug,
                             workload_type,
                             workload_file,
                             schema_file,
                             num_records,
                             scenario_threshold);
        } catch (FileNotFoundException e) {
            throw new AssertionError("Conf file not found");
        }
    }

}
