#ifndef CONSISTENT_HASH_PROVIDER_H
#define CONSISTENT_HASH_PROVIDER_H

#include <string>
#include <vector>
#include <set>
#include <map>
#include <mutex>
#include <thread>
#include <memory>
#include <atomic>
#include <cstdint>
#include <functional>
// Include necessary libraries for etcd and hashing
#include "AlluxioClientConfig.h"

using namespace std;

// Constants
const std::string DEFAULT_HOST = "localhost";
const std::string DEFAULT_CONTAINER_HOST = "";
const int DEFAULT_RPC_PORT = 29999;
const int DEFAULT_DATA_PORT = 29997;
const int DEFAULT_SECURE_RPC_PORT = 0;
const int DEFAULT_NETTY_DATA_PORT = 29997;
const int DEFAULT_WEB_PORT = 30000;
const std::string DEFAULT_DOMAIN_SOCKET_PATH = "";
const int ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE = 28080;
const int DEFAULT_WORKER_IDENTIFIER_VERSION = 1;
const std::string ETCD_PREFIX_FORMAT = "/alluxio/%s/worker/";

// Null namespace UUID (equivalent to Python's uuid.NAMESPACE_OID)
const std::string NULL_NAMESPACE_UUID = "00000000-0000-0000-0000-000000000000";

class WorkerNetAddress {
public:
    std::string host;
    std::string container_host;
    int rpc_port;
    int data_port;
    int secure_rpc_port;
    int netty_data_port;
    int web_port;
    std::string domain_socket_path;
    int http_server_port;

    WorkerNetAddress(
        const std::string& host = DEFAULT_HOST,
        const std::string& container_host = DEFAULT_CONTAINER_HOST,
        int rpc_port = DEFAULT_RPC_PORT,
        int data_port = DEFAULT_DATA_PORT,
        int secure_rpc_port = DEFAULT_SECURE_RPC_PORT,
        int netty_data_port = DEFAULT_NETTY_DATA_PORT,
        int web_port = DEFAULT_WEB_PORT,
        const std::string& domain_socket_path = DEFAULT_DOMAIN_SOCKET_PATH,
        int http_server_port = ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE);
};



class WorkerIdentity {
public:
    int version;
    std::vector<uint8_t> identifier;

    WorkerIdentity(int version, const std::vector<uint8_t>& identifier);

    bool operator==(const WorkerIdentity& other) const;
    bool operator<(const WorkerIdentity& other) const;
};

class WorkerEntity {
public:
    WorkerIdentity worker_identity;
    WorkerNetAddress worker_net_address;

    WorkerEntity(const WorkerIdentity& worker_identity, const WorkerNetAddress& worker_net_address);

    static WorkerEntity from_worker_info(const std::string& worker_info);
    static WorkerEntity from_host_and_port(const std::string& worker_host, int worker_http_port);
};

class EtcdClient {
public:
    EtcdClient(const AlluxioClientConfig& config, const std::string& host, int port);

    std::set<WorkerEntity> get_worker_entities();

private:
    std::string _host;
    int _port;
    std::string _etcd_username;
    std::string _etcd_password;
    std::string _prefix;
};

class ConsistentHashProvider {
public:
    ConsistentHashProvider(const AlluxioClientConfig& config, int max_attempts = 100);

    std::vector<WorkerNetAddress> get_multiple_workers(const std::string& key, int count);

    void shutdown_background_update_ring();

    ~ConsistentHashProvider();

private:
    AlluxioClientConfig _config;
    int _max_attempts;
    std::mutex _lock;
    bool _is_ring_initialized;
    std::map<WorkerIdentity, WorkerNetAddress> _worker_info_map;
    std::map<int64_t, WorkerIdentity> _hash_ring;

    std::atomic<bool> _shutdown_background_update_ring_event;
    std::thread _background_thread;

    void _fetch_workers_and_update_ring();
    void _update_hash_ring(const std::map<WorkerIdentity, WorkerNetAddress>& worker_info_map);
    std::vector<WorkerIdentity> _get_multiple_worker_identities(const std::string& key, int count);
    int64_t _hash(const std::string& key, int index);
    int64_t _hash_worker_identity(const WorkerIdentity& worker, int node_index);
    WorkerIdentity _get_ceiling_value(int64_t hash_key);
    std::map<WorkerIdentity, WorkerNetAddress> _generate_worker_info_map(const std::string& worker_hosts, int worker_http_port);
    void _start_background_update_ring(int interval);
};

#endif // CONSISTENT_HASH_PROVIDER_H
