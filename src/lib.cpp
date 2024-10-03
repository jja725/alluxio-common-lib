#include <alluxio_lib/lib.hpp>
#include <cstdlib> 
#include <iostream>
#include <sstream>
#include <algorithm>
#include <chrono>
#include <thread>
#include <stdexcept>

#include <json/json.h> // Include the JsonCpp header
#include <etcd/Client.hpp>
// Assuming you have a C++ etcd client library
#include <MurmurHash3.h> // Assuming you have a MurmurHash3 implementation

// WorkerNetAddress implementation
WorkerNetAddress::WorkerNetAddress(
    const std::string& host,
    const std::string& container_host,
    int rpc_port,
    int data_port,
    int secure_rpc_port,
    int netty_data_port,
    int web_port,
    const std::string& domain_socket_path,
    int http_server_port)
    : host(host),
      container_host(container_host),
      rpc_port(rpc_port),
      data_port(data_port),
      secure_rpc_port(secure_rpc_port),
      netty_data_port(netty_data_port),
      web_port(web_port),
      domain_socket_path(domain_socket_path),
      http_server_port(http_server_port) {}
// WorkerIdentity implementation
WorkerIdentity::WorkerIdentity(int version, const string& identifier)
    : version(version), identifier(identifier) {}

bool WorkerIdentity::operator==(const WorkerIdentity& other) const {
    return version == other.version && identifier == other.identifier;
}

bool WorkerIdentity::operator<(const WorkerIdentity& other) const {
    if (version != other.version)
        return version < other.version;
    return identifier < other.identifier;
}

// WorkerEntity implementation
WorkerEntity::WorkerEntity(const WorkerIdentity& worker_identity, const WorkerNetAddress& worker_net_address)
    : worker_identity(worker_identity), worker_net_address(worker_net_address) {}

WorkerEntity WorkerEntity::from_worker_info(const std::string& worker_info) {
    try {
        Json::Value root;
        Json::CharReaderBuilder builder;
        std::string errs;
        std::istringstream ss(worker_info);
        if (!Json::parseFromStream(builder, ss, &root, &errs)) {
            throw std::runtime_error("Invalid JSON: " + errs);
        }

        auto identity_info = root["Identity"];
        WorkerIdentity worker_identity(
            identity_info.get("version", DEFAULT_WORKER_IDENTIFIER_VERSION).asInt(),
            identity_info.get("identifier", "").asString());

        auto net_address_info = root["WorkerNetAddress"];
        WorkerNetAddress worker_net_address(
            net_address_info.get("Host", DEFAULT_HOST).asString(),
            net_address_info.get("ContainerHost", DEFAULT_CONTAINER_HOST).asString(),
            net_address_info.get("RpcPort", DEFAULT_RPC_PORT).asInt(),
            net_address_info.get("DataPort", DEFAULT_DATA_PORT).asInt(),
            net_address_info.get("SecureRpcPort", DEFAULT_SECURE_RPC_PORT).asInt(),
            net_address_info.get("NettyDataPort", DEFAULT_NETTY_DATA_PORT).asInt(),
            net_address_info.get("WebPort", DEFAULT_WEB_PORT).asInt(),
            net_address_info.get("DomainSocketPath", DEFAULT_DOMAIN_SOCKET_PATH).asString(),
            net_address_info.get("HttpServerPort", ALLUXIO_WORKER_HTTP_SERVER_PORT_DEFAULT_VALUE).asInt()
        );

        return WorkerEntity(worker_identity, worker_net_address);
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to process worker_info: " + std::string(e.what()));
    }
}

WorkerEntity WorkerEntity::from_host_and_port(const std::string& worker_host, int worker_http_port) {


    WorkerIdentity worker_identity(DEFAULT_WORKER_IDENTIFIER_VERSION, "");
    WorkerNetAddress worker_net_address(
        worker_host,
        DEFAULT_CONTAINER_HOST,
        DEFAULT_RPC_PORT,
        DEFAULT_DATA_PORT,
        DEFAULT_SECURE_RPC_PORT,
        DEFAULT_NETTY_DATA_PORT,
        DEFAULT_WEB_PORT,
        DEFAULT_DOMAIN_SOCKET_PATH,
        worker_http_port
    );
    return WorkerEntity(worker_identity, worker_net_address);
}

// EtcdClient implementation
EtcdClient::EtcdClient(const AlluxioClientConfig& config, const std::string& host, int port)
    : _host(host), _port(port) {
    if (_host.empty()) {
        throw std::invalid_argument("ETCD host must be provided.");
    }
    if (_port == 0) {
        throw std::invalid_argument("ETCD port must be provided.");
    }

    _etcd_username = config.etcd_username;
    _etcd_password = config.etcd_password;

    if ((_etcd_username.empty()) != (_etcd_password.empty())) {
        throw std::invalid_argument("Both ETCD username and password must be set or both unset.");
    }

    char buffer[256];
    snprintf(buffer, sizeof(buffer), ETCD_PREFIX_FORMAT.c_str(), config.cluster_name.c_str());
    _prefix = buffer;
}

std::set<WorkerEntity> EtcdClient::get_worker_entities() {
    // TODO: Implement the logic to fetch worker entities from ETCD
    std::set<WorkerEntity> worker_entities;
    return worker_entities;
}

// ConsistentHashProvider implementation
ConsistentHashProvider::ConsistentHashProvider(const AlluxioClientConfig& config, int max_attempts)
    : _config(config),
      _max_attempts(max_attempts),
      _is_ring_initialized(false),
      _shutdown_background_update_ring_event(false) {

    if (!_config.worker_hosts.empty()) {
        _update_hash_ring(_generate_worker_info_map(_config.worker_hosts, _config.worker_http_port));
    }

    if (!_config.etcd_hosts.empty()) {
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
            } catch (const std::exception& e) {
                std::cerr << "Error updating worker hash ring: " << e.what() << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::seconds(interval));
        }
    });
}

void ConsistentHashProvider::shutdown_background_update_ring() {
    if (!_config.etcd_hosts.empty() && _config.etcd_refresh_workers_interval > 0) {
        _shutdown_background_update_ring_event.store(true);
        if (_background_thread.joinable()) {
            _background_thread.join();
        }
    }
}

ConsistentHashProvider::~ConsistentHashProvider() {
    shutdown_background_update_ring();
}

std::vector<WorkerNetAddress> ConsistentHashProvider::get_multiple_workers(const std::string& key, int count) {
    std::lock_guard<std::mutex> lock(_lock);
    auto worker_identities = _get_multiple_worker_identities(key, count);
    std::vector<WorkerNetAddress> worker_addresses;

    for (const auto& worker_identity : worker_identities) {
        auto it = _worker_info_map.find(worker_identity);
        if (it != _worker_info_map.end()) {
            worker_addresses.push_back(it->second);
        }
    }

    return worker_addresses;
}

std::vector<WorkerIdentity> ConsistentHashProvider::_get_multiple_worker_identities(const std::string& key, int count) {
    count = std::min(count, static_cast<int>(_worker_info_map.size()));
    std::vector<WorkerIdentity> workers;
    int attempts = 0;

    while (workers.size() < static_cast<size_t>(count) && attempts < _max_attempts) {
        attempts++;
        int64_t hash_key = _hash(key, attempts);
        WorkerIdentity worker = _get_ceiling_value(hash_key);
        if (std::find(workers.begin(), workers.end(), worker) == workers.end()) {
            workers.push_back(worker);
        }
    }

    return workers;
}

void ConsistentHashProvider::_fetch_workers_and_update_ring() {
    std::vector<std::string> etcd_hosts_list;
    std::istringstream iss(_config.etcd_hosts);
    std::string host;

    while (std::getline(iss, host, ',')) {
        etcd_hosts_list.push_back(host);
    }

    std::random_shuffle(etcd_hosts_list.begin(), etcd_hosts_list.end());
    std::set<WorkerEntity> worker_entities;

    for (const auto& host : etcd_hosts_list) {
        try {
            EtcdClient etcd_client(_config, host, _config.etcd_port);
            worker_entities = etcd_client.get_worker_entities();
            break;
        } catch (...) {
            continue;
        }
    }

    if (worker_entities.empty()) {
        if (_is_ring_initialized) {
            std::cerr << "Failed to retrieve worker info from ETCD servers: " << _config.etcd_hosts << std::endl;
            return;
        } else {
            throw std::runtime_error("Failed to retrieve worker info from ETCD servers: " + _config.etcd_hosts);
        }
    }

    std::map<WorkerIdentity, WorkerNetAddress> worker_info_map;
    bool diff_detected = false;

    for (const auto& worker_entity : worker_entities) {
        worker_info_map[worker_entity.worker_identity] = worker_entity.worker_net_address;
        auto it = _worker_info_map.find(worker_entity.worker_identity);

        if (it == _worker_info_map.end() || it->second.host != worker_entity.worker_net_address.host) {
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

void ConsistentHashProvider::_update_hash_ring(const std::map<WorkerIdentity, WorkerNetAddress>& worker_info_map) {
    std::lock_guard<std::mutex> lock(_lock);
    _hash_ring.clear();

    for (const auto& [worker_identity, _] : worker_info_map) {
        for (int i = 0; i < _config.hash_node_per_worker; ++i) {
            int64_t hash_key = _hash_worker_identity(worker_identity, i);
            _hash_ring[hash_key] = worker_identity;
        }
    }

    _worker_info_map = worker_info_map;
    _is_ring_initialized = true;
}

WorkerIdentity ConsistentHashProvider::_get_ceiling_value(int64_t hash_key) {
    auto it = _hash_ring.upper_bound(hash_key);
    if (it != _hash_ring.end()) {
        return it->second;
    } else {
        return _hash_ring.begin()->second;
    }
}

int64_t ConsistentHashProvider::_hash(const std::string& key, int index) {
    uint32_t hash[1];
    std::string data = key + std::to_string(index);
    MurmurHash3_x86_32(data.c_str(), data.size(), 0, hash);
    return static_cast<int64_t>(hash[0]);
}

int64_t ConsistentHashProvider::_hash_worker_identity(const WorkerIdentity& worker, int node_index) {
    uint32_t hash[1];
    std::string data(worker.identifier.begin(), worker.identifier.end());
    data += std::to_string(worker.version) + std::to_string(node_index);
    MurmurHash3_x86_32(data.c_str(), data.size(), 0, hash);
    return static_cast<int64_t>(hash[0]);
}

std::map<WorkerIdentity, WorkerNetAddress> ConsistentHashProvider::_generate_worker_info_map(const std::string& worker_hosts, int worker_http_port) {
    std::map<WorkerIdentity, WorkerNetAddress> worker_info_map;
    std::istringstream iss(worker_hosts);
    std::string worker_host;

    while (std::getline(iss, worker_host, ',')) {
        worker_host.erase(std::remove_if(worker_host.begin(), worker_host.end(), ::isspace), worker_host.end());
        WorkerEntity worker_entity = WorkerEntity::from_host_and_port(worker_host, worker_http_port);
        worker_info_map[worker_entity.worker_identity] = worker_entity.worker_net_address;
    }

    return worker_info_map;
}



// Constructor
AlluxioClient::AlluxioClient(const std::string& masterAddress, int port)
    : m_masterAddress(masterAddress), m_port(port) {
    // Initialization code if required
    // For example, establish connection to Alluxio master
}

// Destructor
AlluxioClient::~AlluxioClient() {
    // Cleanup code if required
    // For example, close connection to Alluxio master
}

std::vector<ReadResponse> AlluxioClient::getWorkerAddress(
    const std::string& filename,
    size_t offset,
    size_t bytes
) {
    // Validate input parameters
    if (filename.empty()) {
        throw std::invalid_argument("Filename cannot be empty.");
    }

    // Call the helper function to query Alluxio
    return queryAlluxioForWorkerAddresses(filename, offset, bytes);
}

std::vector<ReadResponse> AlluxioClient::queryAlluxioForWorkerAddresses(
    const std::string& filename,
    size_t offset,
    size_t bytes
) {
    // Vector to store the resulting worker addresses
    std::vector<ReadResponse> workerAddresses;

    // TODO: Implement the logic to interact with Alluxio to retrieve worker addresses.
    // This may involve using Alluxio's client APIs or sending RPCs to the master.

    // For demonstration purposes, we'll create some dummy data
    ReadResponse address1;
    address1.start_offset = offset;
    address1.bytes = bytes / 2;  // Assume half the bytes are handled by this worker
    address1.IPs = {"10.0.0.1", "10.0.0.2"};

    ReadResponse address2;
    address2.start_offset = offset + bytes / 2;
    address2.bytes = bytes - address1.bytes;  // The remaining bytes
    address2.IPs = {"10.0.0.3", "10.0.0.4"};

    // Add the dummy addresses to the vector
    workerAddresses.push_back(address1);
    workerAddresses.push_back(address2);

    // Return the list of worker addresses
    return workerAddresses;
}