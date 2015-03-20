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
    public final String connection_pool_type;
    public final int num_client_threads;
    public final int status_thread_update_interval_ms;
    public final boolean debug;
    public final WorkloadType workload_type;
    public final String workload_file;
    public final String schema_file;
    public final long num_records;
    public final long record_start;
    public final int workload_gen_throttle;

    private Conf(String cluster_name,
                 String keyspace_name,
                 String column_family_name,
                 int max_conns,
                 String hosts,
                 int async_executor_num_threads,
                 String connection_pool_type,
                 int num_client_threads,
                 int status_thread_update_interval_ms,
                 boolean debug,
                 WorkloadType workload_type,
                 String workload_file,
                 String schema_file,
                 long num_records,
                 long record_start,
                 int workload_gen_throttle) {
        this.cluster_name = cluster_name;
        this.keyspace_name = keyspace_name;
        this.column_family_name = column_family_name;
        this.max_conns = max_conns;
        this.async_executor_num_threads = async_executor_num_threads;
        this.connection_pool_type = connection_pool_type;
        this.hosts = hosts;
        this.num_client_threads = num_client_threads;
        this.status_thread_update_interval_ms = status_thread_update_interval_ms;
        this.debug = debug;
        this.workload_type = workload_type;
        this.workload_file = workload_file;
        this.schema_file = schema_file;
        this.num_records = num_records;
        this.record_start = record_start;
        this.workload_gen_throttle = workload_gen_throttle;
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

            String connection_pool_type = (String) conf_map.get("connection_pool_type");
            assert connection_pool_type != null;

            int num_client_threads = (int) conf_map.get("num_client_threads");
            assert num_client_threads > 0;

            int status_thread_update_interval_ms = (int) conf_map.get("status_thread_update_interval_ms");
            assert status_thread_update_interval_ms > 0;

            boolean debug = (boolean) conf_map.get("debug");

            String schema_file = (String) conf_map.get("schema_file");
            assert schema_file != null;
            assert !schema_file.isEmpty();

            WorkloadType workload_type = WorkloadType.valueOf((String) conf_map.get("workload_type"));

            long num_records = -1;
            long record_start = -1;
            String workload_file = null;

            if (workload_type.equals(WorkloadType.SYNTHETIC)) {
                num_records = Long.valueOf(String.valueOf(conf_map.get("num_records")));
                assert num_records > 0;

                record_start = Long.valueOf(String.valueOf(conf_map.get("record_start")));
                assert record_start > 0;
            } else if (workload_type.equals(WorkloadType.FILE)) {
                workload_file = (String) conf_map.get("workload_file");
                assert workload_file != null;
                assert !workload_file.isEmpty();
            } else {
                throw new AssertionError("Cannot handle WorkloadType" + workload_type);
            }

            int workload_gen_throttle = (int) conf_map.get("workload_gen_throttle");
            assert workload_gen_throttle > 0;

            return new Conf (cluster_name,
                             keyspace_name,
                             column_family_name,
                             max_conns,
                             hosts,
                             async_executor_num_threads,
                             connection_pool_type,
                             num_client_threads,
                             status_thread_update_interval_ms,
                             debug,
                             workload_type,
                             workload_file,
                             schema_file,
                             num_records,
                             record_start,
                             workload_gen_throttle);
        } catch (FileNotFoundException e) {
            throw new AssertionError("Conf file not found");
        }
    }

}
