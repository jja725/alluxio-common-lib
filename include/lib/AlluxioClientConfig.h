#ifndef ALLUXIOCLIENTCONFIG_H
#define ALLUXIOCLIENTCONFIG_H

#include <string>
#include <stdexcept>
#include <cstdint>
using namespace std;

struct AlluxioClientConfig {
    // Constructor
    AlluxioClientConfig(
        const std::string& etcd_hosts = "",
        const std::string& worker_hosts = "",
        int etcd_port = 2379,
        int worker_http_port = 30001,
        int etcd_refresh_workers_interval = 120,
        int hash_node_per_worker = 5,
        const std::string& cluster_name = "default",
        const std::string& etcd_username = "",
        const std::string& etcd_password = "")
        : etcd_hosts(etcd_hosts),
          worker_hosts(worker_hosts),
          etcd_port(etcd_port),
          worker_http_port(worker_http_port),
          etcd_refresh_workers_interval(etcd_refresh_workers_interval),
          hash_node_per_worker(hash_node_per_worker),
          cluster_name(cluster_name),
          etcd_username(etcd_username),
          etcd_password(etcd_password) {}

    // Public member variables
    std::string etcd_hosts;
    std::string worker_hosts;
    int etcd_port;
    int worker_http_port;
    int etcd_refresh_workers_interval;
    int hash_node_per_worker;
    std::string cluster_name;
    std::string etcd_username;
    std::string etcd_password;
};

#endif // ALLUXIOCLIENTCONFIG_H
