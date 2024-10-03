#pragma once

#include <string>
#include <vector>
#include <set>
#include <map>
#include <thread>
#include <atomic>
#include <cstdint>

using namespace std;

// Constants
const string DEFAULT_HOST = "localhost";
const string DEFAULT_CONTAINER_HOST = "";
const int DEFAULT_RPC_PORT = 29999;
const int DEFAULT_DATA_PORT = 29997;
const int DEFAULT_SECURE_RPC_PORT = 0;
const int DEFAULT_NETTY_DATA_PORT = 29997;
const int DEFAULT_WEB_PORT = 30000;
const string DEFAULT_DOMAIN_SOCKET_PATH = "";
const int ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE = 28080;
const int DEFAULT_WORKER_IDENTIFIER_VERSION = 1;
const string ETCD_PREFIX_FORMAT = "/alluxio/%s/worker/";

// Null namespace UUID (equivalent to Python's uuid.NAMESPACE_OID)
const string NULL_NAMESPACE_UUID = "00000000-0000-0000-0000-000000000000";



struct AlluxioClientConfig {
    // Constructor
    AlluxioClientConfig(
        const string& etcd_hosts = "",
        const string& worker_hosts = "",
        const int etcd_port = 2379,
        const int worker_http_port = 30001,
        const int etcd_refresh_workers_interval = 120,
        const int hash_node_per_worker = 5,
        const string& cluster_name = "default",
        const string& etcd_username = "",
        const string& etcd_password = "")
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
    string etcd_hosts;
    string worker_hosts;
    int etcd_port;
    int worker_http_port;
    int etcd_refresh_workers_interval;
    int hash_node_per_worker;
    string cluster_name;
    string etcd_username;
    string etcd_password;
};

class WorkerNetAddress {
public:
    string host;
    string container_host;
    int rpc_port;
    int data_port;
    int secure_rpc_port;
    int netty_data_port;
    int web_port;
    string domain_socket_path;
    int http_server_port;

    WorkerNetAddress(
        const string& host = DEFAULT_HOST,
        const string& container_host = DEFAULT_CONTAINER_HOST,
        int rpc_port = DEFAULT_RPC_PORT,
        int data_port = DEFAULT_DATA_PORT,
        int secure_rpc_port = DEFAULT_SECURE_RPC_PORT,
        int netty_data_port = DEFAULT_NETTY_DATA_PORT,
        int web_port = DEFAULT_WEB_PORT,
        const string& domain_socket_path = DEFAULT_DOMAIN_SOCKET_PATH,
        int http_server_port = ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE);
};

struct WorkerIdentity {
    int version;
    string identifier;

    WorkerIdentity(int version, const string& identifier);

    bool operator==(const WorkerIdentity& other) const;
    bool operator<(const WorkerIdentity& other) const;
};

class WorkerEntity {
public:
    WorkerIdentity worker_identity;
    WorkerNetAddress worker_net_address;

    WorkerEntity(const WorkerIdentity& worker_identity, const WorkerNetAddress& worker_net_address);

    static WorkerEntity from_worker_info(const string& worker_info);
    static WorkerEntity from_host_and_port(const string& worker_host, int worker_http_port);
};

class EtcdClient {
public:
    EtcdClient(const AlluxioClientConfig& config, const string& host, int port);

    set<WorkerEntity> get_worker_entities();

private:
    string _host;
    int _port;
    string _etcd_username;
    string _etcd_password;
    string _prefix;
};

class ConsistentHashProvider {
public:
    ConsistentHashProvider(const AlluxioClientConfig& config, int max_attempts = 100);

    vector<WorkerNetAddress> get_multiple_workers(const string& key, int count);

    void shutdown_background_update_ring();

    ~ConsistentHashProvider();

private:
    AlluxioClientConfig _config;
    int _max_attempts;
    mutex _lock;
    bool _is_ring_initialized;
    map<WorkerIdentity, WorkerNetAddress> _worker_info_map;
    map<int64_t, WorkerIdentity> _hash_ring;

    atomic<bool> _shutdown_background_update_ring_event;
    thread _background_thread;

    void _fetch_workers_and_update_ring();
    void _update_hash_ring(const map<WorkerIdentity, WorkerNetAddress>& worker_info_map);
    vector<WorkerIdentity> _get_multiple_worker_identities(const string& key, int count);
    int64_t _hash(const string& key, int index);
    int64_t _hash_worker_identity(const WorkerIdentity& worker, int node_index);
    WorkerIdentity _get_ceiling_value(int64_t hash_key);
    map<WorkerIdentity, WorkerNetAddress> _generate_worker_info_map(const string& worker_hosts, int worker_http_port);
    void _start_background_update_ring(int interval);
};


// Structure to hold worker address information
struct ReadResponse {
    size_t start_offset;             // The starting offset
    size_t bytes;                    // Number of bytes
    vector<string> IPs;    // List of IP addresses
};

class AlluxioClient {
public:
    // Constructor
    AlluxioClient(const string& masterAddress, int port);

    // Destructor
    ~AlluxioClient();

    /**
     * @brief Retrieves worker addresses for a given file segment from Alluxio.
     *
     * @param filename The name of the file.
     * @param offset The starting offset within the file.
     * @param bytes The number of bytes to process.
     * @return A list of WorkerAddress structures.
     */
    vector<ReadResponse> getWorkerAddress(
        const string& filename,
        size_t offset,
        size_t bytes
    );

private:
    string m_masterAddress;  // Alluxio master address
    int m_port;                   // Port number

    // Private helper functions
    vector<ReadResponse> queryAlluxioForWorkerAddresses(
        const string& filename,
        size_t offset,
        size_t bytes
    );
};