#include <MurmurHash3.h> // Assuming you have a MurmurHash3 implementation
#include <algorithm>
#include <alluxio_lib/lib.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <etcd/Client.hpp> // Assuming you have a C++ etcd client library
#include <fmt/core.h>
#include <json/json.h> // Include the JsonCpp header
#include <string>

namespace alluxio {
// WorkerNetAddress implementation

WorkerNetAddress::WorkerNetAddress(
    const std::string &host, const std::string &container_host, int rpc_port,
    int data_port, int secure_rpc_port, int netty_data_port, int web_port,
    const std::string &domain_socket_path, int http_server_port)
    : host(host), container_host(container_host), rpc_port(rpc_port),
      data_port(data_port), secure_rpc_port(secure_rpc_port),
      netty_data_port(netty_data_port), web_port(web_port),
      domain_socket_path(domain_socket_path),
      http_server_port(http_server_port) {}

// WorkerIdentity struct implementation
WorkerIdentity::WorkerIdentity(int version, std::vector<char> &identifier)
    : version(version), identifier(identifier) {}

std::vector<char>
WorkerIdentity::get_bytes_from_hex_string(std::string identifier_hex) {
  std::vector<char> identifier;
  for (size_t i = 0; i < identifier_hex.length(); i += 2) {
    identifier.push_back(
        static_cast<char>(std::stoi(identifier_hex.substr(i, 2), nullptr, 16)));
  }
  return identifier;
}

bool WorkerIdentity::operator==(const WorkerIdentity &other) const {
  return version == other.version && identifier == other.identifier;
}

bool WorkerIdentity::operator<(const WorkerIdentity &other) const {
  if (version != other.version)
    return version < other.version;
  return identifier < other.identifier;
}

// WorkerEntity implementation
WorkerEntity::WorkerEntity(const WorkerIdentity &worker_identity,
                           const WorkerNetAddress &worker_net_address)
    : worker_identity(worker_identity), worker_net_address(worker_net_address) {
}

WorkerEntity WorkerEntity::from_worker_info(const std::string &worker_info) {
  try {
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errs;
    std::istringstream ss(worker_info);
    if (!Json::parseFromStream(builder, ss, &root, &errs)) {
      throw std::runtime_error("Invalid JSON: " + errs);
    }

    auto identity_info = root["Identity"];
    std::string identifier_hex = identity_info.get("identifier", "").asString();

    auto identifier = WorkerIdentity::get_bytes_from_hex_string(identifier_hex);
    boost::uuids::uuid u;
    std::vector<char> v(u.size());
    std::copy(u.begin(), u.end(), v.begin());
    WorkerIdentity worker_identity(
        identity_info.get("version", DEFAULT_WORKER_IDENTIFIER_VERSION).asInt(),
        identifier.empty() ? v : identifier);

    auto net_address_info = root["WorkerNetAddress"];
    WorkerNetAddress worker_net_address(
        net_address_info.get("Host", DEFAULT_HOST).asString(),
        net_address_info.get("ContainerHost", DEFAULT_CONTAINER_HOST)
            .asString(),
        net_address_info.get("RpcPort", DEFAULT_RPC_PORT).asInt(),
        net_address_info.get("DataPort", DEFAULT_DATA_PORT).asInt(),
        net_address_info.get("SecureRpcPort", DEFAULT_SECURE_RPC_PORT).asInt(),
        net_address_info.get("NettyDataPort", DEFAULT_NETTY_DATA_PORT).asInt(),
        net_address_info.get("WebPort", DEFAULT_WEB_PORT).asInt(),
        net_address_info.get("DomainSocketPath", DEFAULT_DOMAIN_SOCKET_PATH)
            .asString(),
        net_address_info
            .get("HttpServerPort",
                 ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE)
            .asInt());

    return WorkerEntity(worker_identity, worker_net_address);
  } catch (const std::exception &e) {
    throw std::runtime_error("Failed to process worker_info: " +
                             std::string(e.what()));
  }
}

WorkerEntity WorkerEntity::from_host_and_port(const std::string &worker_host,
                                              int worker_http_port) {
  boost::uuids::uuid u;
  std::vector<char> v(u.size());
  std::copy(u.begin(), u.end(), v.begin());
  WorkerIdentity worker_identity(DEFAULT_WORKER_IDENTIFIER_VERSION, v);
  WorkerNetAddress worker_net_address(
      worker_host, DEFAULT_CONTAINER_HOST, DEFAULT_RPC_PORT, DEFAULT_DATA_PORT,
      DEFAULT_SECURE_RPC_PORT, DEFAULT_NETTY_DATA_PORT, DEFAULT_WEB_PORT,
      DEFAULT_DOMAIN_SOCKET_PATH, worker_http_port);
  return WorkerEntity(worker_identity, worker_net_address);
}

// EtcdClient implementation
EtcdClient::EtcdClient(const AlluxioClientConfig &config,
                       const std::string &etcd_urls)
    : _host(etcd_urls) {
  if (_host.empty()) {
    throw std::invalid_argument("ETCD host must be provided.");
  }

  _etcd_username = config.etcd_username;
  _etcd_password = config.etcd_password;

  if ((_etcd_username.empty()) != (_etcd_password.empty())) {
    throw std::invalid_argument(
        "Both ETCD username and password must be set or both unset.");
  }
  _prefix = fmt::format(ETCD_PREFIX_FORMAT, config.cluster_name);
  if (_etcd_username.empty()) {
    _etcd_client = std::make_shared<etcd::Client>(etcd_urls);
  } else {
    _etcd_client = std::make_shared<etcd::Client>(etcd_urls, _etcd_username,
                                                  _etcd_password);
  }
}

std::vector<WorkerEntity> EtcdClient::get_worker_entities() const {
  std::vector<WorkerEntity> worker_entities;

  try {
    etcd::Response response = _etcd_client->ls(_prefix).get();
    if (!response.is_ok()) {
      throw std::runtime_error(
          fmt::format("Failed to get worker entities given prefix {} : {}",
                      _prefix, response.error_message()));
    }

    for (const auto &kv : response.values()) {
      try {
        auto entity_str = kv.as_string();
        auto worker_entity = WorkerEntity::from_worker_info(entity_str);
        worker_entities.push_back(worker_entity);
      } catch (const std::exception &e) {
        std::cerr << "Failed to process worker info: " << e.what() << std::endl;
      }
    }
    if (worker_entities.empty()) {
      throw std::runtime_error(
          "Alluxio cluster may still be initializing. No worker registered");
    }
  } catch (const std::exception &e) {
    std::cerr << "ETCD error: " << e.what() << std::endl;
    throw std::runtime_error("ETCD error: " + std::string(e.what()));
  } catch (...) {
    std::cerr << "Unknown ETCD error occurred" << std::endl;
  }

  return worker_entities;
}

// ConsistentHashProvider implementation
ConsistentHashProvider::ConsistentHashProvider(
    const AlluxioClientConfig &config, int max_attempts)
    : _config(config), _max_attempts(max_attempts), _is_ring_initialized(false),
      _shutdown_background_update_ring_event(false),
      _etcd_client(config, config.etcd_urls) {
  if (!_config.etcd_urls.empty()) {
    _fetch_workers_and_update_ring();
    if (_config.etcd_refresh_workers_interval > 0) {
      _start_background_update_ring(_config.etcd_refresh_workers_interval);
    }
  }
}

void ConsistentHashProvider::_start_background_update_ring(int interval) {
  _background_thread = std::thread([this, interval]() {
    while (!_shutdown_background_update_ring_event.load()) {
      try {
        _fetch_workers_and_update_ring();
      } catch (const std::exception &e) {
        std::cerr << "Error updating worker hash ring: " << e.what()
                  << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::seconds(interval));
    }
  });
}

void ConsistentHashProvider::shutdown_background_update_ring() {
  if (!_config.etcd_urls.empty() && _config.etcd_refresh_workers_interval > 0) {
    _shutdown_background_update_ring_event.store(true);
    if (_background_thread.joinable()) {
      _background_thread.join();
    }
    std::cout << "Background update ring thread joined" << std::endl;
  }
}

ConsistentHashProvider::~ConsistentHashProvider() {
  shutdown_background_update_ring();
}

std::vector<WorkerNetAddress>
ConsistentHashProvider::get_multiple_workers(const std::string &key,
                                             int count) {
  std::lock_guard<std::mutex> lock(_lock);
  auto worker_identities = _get_multiple_worker_identities(key, count);
  std::vector<WorkerNetAddress> worker_addresses;

  for (const auto &worker_identity : worker_identities) {
    auto it = _worker_info_map.find(worker_identity);
    if (it != _worker_info_map.end()) {
      worker_addresses.push_back(it->second);
    }
  }

  return worker_addresses;
}

std::vector<WorkerIdentity>
ConsistentHashProvider::_get_multiple_worker_identities(const std::string &key,
                                                        int count) {
  count = std::min(count, static_cast<int>(_worker_info_map.size()));
  std::vector<WorkerIdentity> workers;
  int attempts = 0;

  while (workers.size() < static_cast<size_t>(count) &&
         attempts < _max_attempts) {
    attempts++;
    auto hash_key = _hash(key, attempts);
    WorkerIdentity worker = _get_ceiling_value(hash_key);
    if (std::find(workers.begin(), workers.end(), worker) == workers.end()) {
      workers.push_back(worker);
    }
  }

  return workers;
}

void ConsistentHashProvider::_fetch_workers_and_update_ring() {
  std::vector<WorkerEntity> worker_entities;
  try {
    worker_entities = _etcd_client.get_worker_entities();
  } catch (const std::exception &e) {
    std::cerr << "Connection error to ETCD url " << _config.etcd_urls << ": "
              << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown error occurred while connecting to ETCD host "
              << _config.etcd_urls << std::endl;
  }

  if (worker_entities.empty()) {
    if (_is_ring_initialized) {
      std::cerr << "Failed to retrieve worker info from ETCD servers: "
                << _config.etcd_urls << std::endl;
      return;
    } else {
      throw std::runtime_error(
          "Failed to retrieve worker info from ETCD servers: " +
          _config.etcd_urls);
    }
  }

  std::unordered_map<WorkerIdentity, WorkerNetAddress> worker_info_map;
  bool diff_detected = false;

  for (const auto &worker_entity : worker_entities) {
    worker_info_map[worker_entity.worker_identity] =
        worker_entity.worker_net_address;
    auto it = _worker_info_map.find(worker_entity.worker_identity);

    if (it == _worker_info_map.end() ||
        it->second.host != worker_entity.worker_net_address.host) {
      diff_detected = true;
    }
  }

  if (worker_info_map.size() != _worker_info_map.size()) {
    diff_detected = true;
  }

  if (diff_detected) {
    _update_hash_ring(worker_info_map);
  }
}

void ConsistentHashProvider::_update_hash_ring(
    const std::unordered_map<WorkerIdentity, WorkerNetAddress>
        &worker_info_map) {
  std::lock_guard<std::mutex> lock(_lock);
  _hash_ring.clear();

  for (const auto &worker_info : worker_info_map) {
    const WorkerIdentity worker_identity = worker_info.first;
    for (int i = 0; i < _config.hash_node_per_worker; ++i) {
      auto hash_key = _hash_worker_identity(worker_identity, i);
      _hash_ring.insert({hash_key, worker_identity});
    }
  }

  _worker_info_map = worker_info_map;
  _is_ring_initialized = true;
}

WorkerIdentity ConsistentHashProvider::_get_ceiling_value(int32_t hash_key) {
  auto it = _hash_ring.upper_bound(hash_key);
  if (it != _hash_ring.end()) {
    return it->second;
  } else {
    return _hash_ring.begin()->second;
  }
}

int32_t ConsistentHashProvider::_hash(const std::string &key, int index) {
  std::vector<uint8_t> buffer;

  // Add key
  buffer.insert(buffer.end(), key.begin(), key.end());

  // Add index
  uint32_t uint_index = static_cast<uint32_t>(index);
  for (int i = 0; i < 4; ++i) {
    buffer.push_back(static_cast<uint8_t>((uint_index >> (i * 8)) & 0xFF));
  }

  // Compute the hash
  int32_t hash;
  MurmurHash3_x86_32(buffer.data(), buffer.size(), 0, &hash);
  return hash;
}

int32_t
ConsistentHashProvider::_hash_worker_identity(const WorkerIdentity &worker,
                                              int node_index) {
  std::vector<uint8_t> buffer;

  // Add worker.mId
  buffer.insert(buffer.end(), worker.identifier.begin(),
                worker.identifier.end());

  // Add worker.mVersion
  uint32_t version = static_cast<uint32_t>(worker.version);
  for (int j = 0; j < 4; ++j) {
    buffer.push_back(static_cast<uint8_t>((version >> (j * 8)) & 0xFF));
  }

  // Add i
  uint32_t index = static_cast<uint32_t>(node_index);
  for (int j = 0; j < 4; ++j) {
    buffer.push_back(static_cast<uint8_t>((index >> (j * 8)) & 0xFF));
  }

  // Compute the hash
  int32_t hash;
  MurmurHash3_x86_32(buffer.data(), buffer.size(), 0, &hash);
  return hash;
}

// Constructor
AlluxioClient::AlluxioClient(const AlluxioClientConfig &config)
    : config(config), hashProvider(config) {
  // Initialization code if required
}

// Destructor
AlluxioClient::~AlluxioClient() {}

std::vector<ReadLocation>
AlluxioClient::getWorkerAddress(const std::string &filename, size_t offset,
                                size_t bytes) {
  // Validate input parameters
  if (filename.empty()) {
    throw std::invalid_argument("Filename cannot be empty.");
  }

  // Use the filename as the key to get worker addresses
  std::vector<WorkerNetAddress> workers =
      hashProvider.get_multiple_workers(filename, 1);

  // Create and return the ReadLocation
  std::vector<ReadLocation> result;
  if (!workers.empty()) {
    ReadLocation location;
    location.start_offset = offset;
    location.bytes = bytes;
    location.workers = workers;
    result.push_back(location);
  }

  return result;
}
} // namespace alluxio