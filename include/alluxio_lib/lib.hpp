#pragma once

#include <atomic>
#include <cstdint>
#include <etcd/Client.hpp>
#include <string>
#include <thread>
#include <vector>

namespace alluxio {
// Constants
static const std::string DEFAULT_HOST = "localhost";
static const std::string DEFAULT_CONTAINER_HOST = "";
static const int DEFAULT_RPC_PORT = 29999;
static const int DEFAULT_DATA_PORT = 29997;
static const int DEFAULT_SECURE_RPC_PORT = 0;
static const int DEFAULT_NETTY_DATA_PORT = 29997;
static const int DEFAULT_WEB_PORT = 30000;
static const std::string DEFAULT_DOMAIN_SOCKET_PATH = "";
static const int ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE = 28080;
static const int DEFAULT_NUM_VIRTUAL_NODES = 2000;
static const int DEFAULT_WORKER_IDENTIFIER_VERSION = 1;
static const std::string ETCD_PREFIX_FORMAT = "/ServiceDiscovery/{}";

struct AlluxioClientConfig {
  // Constructor
  AlluxioClientConfig(
      const std::string &etcd_hosts = "localhost:2379",
      const std::string &worker_hosts = "", const int worker_http_port = 30001,
      const int etcd_refresh_workers_interval = 120,
      const int hash_node_per_worker = DEFAULT_NUM_VIRTUAL_NODES,
      const std::string &cluster_name = "alluxio",
      const std::string &etcd_username = "",
      const std::string &etcd_password = "")
      : etcd_urls(etcd_hosts), worker_hosts(worker_hosts),
        worker_http_port(worker_http_port),
        etcd_refresh_workers_interval(etcd_refresh_workers_interval),
        hash_node_per_worker(hash_node_per_worker), cluster_name(cluster_name),
        etcd_username(etcd_username), etcd_password(etcd_password) {}

  // Public member variables
  std::string etcd_urls;
  std::string worker_hosts;
  int worker_http_port;
  int etcd_refresh_workers_interval;
  int hash_node_per_worker;
  std::string cluster_name;
  std::string etcd_username;
  std::string etcd_password;
};

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
      const std::string &host = DEFAULT_HOST,
      const std::string &container_host = DEFAULT_CONTAINER_HOST,
      int rpc_port = DEFAULT_RPC_PORT, int data_port = DEFAULT_DATA_PORT,
      int secure_rpc_port = DEFAULT_SECURE_RPC_PORT,
      int netty_data_port = DEFAULT_NETTY_DATA_PORT,
      int web_port = DEFAULT_WEB_PORT,
      const std::string &domain_socket_path = DEFAULT_DOMAIN_SOCKET_PATH,
      int http_server_port = ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE);
};

struct WorkerIdentity {
  int version;
  std::vector<char> identifier;

  WorkerIdentity(int version, std::vector<char> &identifier);
  static std::vector<char>
  get_bytes_from_hex_string(std::string identifier_hex);
  bool operator==(const WorkerIdentity &other) const;
  bool operator<(const WorkerIdentity &other) const;
};
} // namespace alluxio
namespace std {
template <> class hash<alluxio::WorkerIdentity> {
public:
  size_t operator()(const alluxio::WorkerIdentity &wi) const {
    size_t h = 0;
    for (char c : wi.identifier) {
      h = h * 31 + c;
    }
    return h * 31 + wi.version;
  }
};
} // namespace std

namespace alluxio {
class WorkerEntity {
public:
  WorkerIdentity worker_identity;
  WorkerNetAddress worker_net_address;

  WorkerEntity(const WorkerIdentity &worker_identity,
               const WorkerNetAddress &worker_net_address);

  static WorkerEntity from_worker_info(const std::string &worker_info);
  static WorkerEntity from_host_and_port(const std::string &worker_host,
                                         int worker_http_port);

  bool operator==(const WorkerEntity &other) const {
    return worker_identity == other.worker_identity &&
           worker_net_address.host == other.worker_net_address.host &&
           worker_net_address.http_server_port ==
               other.worker_net_address.http_server_port;
  }
};

class EtcdClient {
public:
  EtcdClient(const AlluxioClientConfig &config, const std::string &etcd_urls);

  std::vector<WorkerEntity> get_worker_entities() const;

private:
  std::string _host;
  std::string _etcd_username;
  std::string _etcd_password;
  std::string _prefix;
  std::shared_ptr<etcd::Client> _etcd_client;
};

class ConsistentHashProvider {
public:
  ConsistentHashProvider(const AlluxioClientConfig &config,
                         int max_attempts = 100);

  std::vector<WorkerNetAddress> get_multiple_workers(const std::string &key,
                                                     int count);

  void shutdown_background_update_ring();

  ~ConsistentHashProvider();

  // for testing
  std::map<int32_t, WorkerIdentity> _hash_ring;
  void
  _update_hash_ring(const std::unordered_map<WorkerIdentity, WorkerNetAddress>
                        &worker_info_map);
  std::vector<WorkerIdentity>
  _get_multiple_worker_identities(const std::string &key, int count);
  int32_t _hash_worker_identity(const WorkerIdentity &worker, int node_index);

private:
  std::unordered_map<WorkerIdentity, WorkerNetAddress> _worker_info_map;
  EtcdClient _etcd_client;
  std::atomic<bool> _shutdown_background_update_ring_event;
  std::thread _background_thread;
  AlluxioClientConfig _config;
  int _max_attempts;
  std::mutex _lock;
  bool _is_ring_initialized;
  void _fetch_workers_and_update_ring();
  void _start_background_update_ring(int interval);
  int32_t _hash(const std::string &key, int index);
  WorkerIdentity _get_ceiling_value(int32_t hash_key);
};

// Structure to hold worker address information
struct ReadLocation {
  size_t start_offset;                   // The starting offset
  size_t bytes;                          // Number of bytes
  std::vector<WorkerNetAddress> workers; // List of IP addresses
};

class AlluxioClient {
public:
  // Constructor
  AlluxioClient(const AlluxioClientConfig &config);

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
  std::vector<ReadLocation> getWorkerAddress(const std::string &filename,
                                             size_t offset, size_t bytes);

private:
  AlluxioClientConfig config;
  ConsistentHashProvider
      hashProvider; // Added ConsistentHashProvider as a private field
};
} // namespace alluxio