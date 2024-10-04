#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>
#include <alluxio_lib/lib.hpp>
#include <json/json.h>
#include <string>
#include <unordered_map>
#include <unordered_set>

// Helper function to convert CamelCase to snake_case
std::string camelToSnake(const std::string& name) {
    std::string result;
    for (char c : name) {
        if (std::isupper(c)) {
            if (!result.empty()) {
                result += '_';
            }
            result += std::tolower(c);
        } else {
            result += c;
        }
    }
    return result;
}

void validate_hash_ring(const std::map<int32_t, WorkerIdentity>& current_ring, const std::string& result_file_path) {
    std::ifstream hash_ring_file(result_file_path);
    REQUIRE(hash_ring_file.is_open());

    Json::Value hash_ring_data;
    hash_ring_file >> hash_ring_data;
    hash_ring_file.close();

    int not_found_count = 0;
    int mismatch_count = 0;


    for (const auto& member : hash_ring_data.getMemberNames()) {
        auto key = std::stoll(member);
        const auto& worker_identity = hash_ring_data[member];
        auto identifier_hex = worker_identity["identifier"].asString();
        auto identifier = WorkerIdentity::get_bytes_from_hex_string(identifier_hex);
        auto version = worker_identity["version"].asInt();
        auto it = current_ring.find(key);
        if (it != current_ring.end()) {
            const auto& current_worker_identity = it->second;
            if (current_worker_identity.version ==  version &&
                current_worker_identity.identifier == identifier) {
                continue;
                } else {
                    mismatch_count++;
                }
        } else {
            not_found_count++;
        }
    }

    REQUIRE(not_found_count == 0);
    REQUIRE(mismatch_count == 0);
}

class ConsistentHashProviderFixture {
protected:
    std::string current_dir;
    std::string hash_res_dir;
    AlluxioClientConfig config;

    ConsistentHashProviderFixture() {
        current_dir = __FILE__;
        current_dir = current_dir.substr(0, current_dir.find_last_of("/\\"));
        hash_res_dir = current_dir + "/fixtures";

        config.hash_node_per_worker = 5;
        config.etcd_refresh_workers_interval = 100000000;
    }
};

TEST_CASE_METHOD(ConsistentHashProviderFixture, "Test hash ring", "[ConsistentHashProvider]") {

    ConsistentHashProvider hash_provider(config);

    // Load worker list and update hash ring
    std::string worker_list_path = hash_res_dir + "/workerList.json";
    std::ifstream worker_list_file(worker_list_path);
    REQUIRE(worker_list_file.is_open());

    Json::Value workers_data;
    worker_list_file >> workers_data;
    worker_list_file.close();

    std::map<WorkerIdentity, WorkerNetAddress> worker_info_map;
    for (const auto& worker_data : workers_data) {
        auto identifier = WorkerIdentity::get_bytes_from_hex_string(worker_data["identifier"].asString());
        WorkerIdentity worker_identity(
            worker_data["version"].asInt(),
            identifier
        );
        WorkerNetAddress default_worker_net_address;
        worker_info_map[worker_identity] = default_worker_net_address;
    }

    hash_provider._update_hash_ring(worker_info_map);
    std::string hash_ring_path = hash_res_dir + "/activeNodesMap.json";
    validate_hash_ring(hash_provider._hash_ring, hash_ring_path);

    // Test file workers
    std::string file_workers_path = hash_res_dir + "/fileUrlWorkers.json";
    std::ifstream file_workers_file(file_workers_path);
    REQUIRE(file_workers_file.is_open());

    Json::Value file_workers_data;
    file_workers_file >> file_workers_data;
    file_workers_file.close();

    for (const auto& member : file_workers_data.getMemberNames()) {
        std::string ufs_url = member;
        Json::Value workers = file_workers_data[member];

        std::vector<WorkerIdentity> current_worker_identities = hash_provider._get_multiple_worker_identities(ufs_url, DEFAULT_NUM_VIRTUAL_NODES);

        std::set<std::pair<int, std::vector<char>>> original_set;
        for (const auto& worker : workers) {
            auto identifier = WorkerIdentity::get_bytes_from_hex_string(worker["identifier"].asString());
            original_set.insert(std::make_pair(worker["version"].asInt(), identifier));
        }

        std::set<std::pair<int, std::vector<char>>> current_set;
        for (const auto& worker : current_worker_identities) {
            current_set.insert(std::make_pair(worker.version, worker.identifier));
        }

        REQUIRE(original_set == current_set);
    }
}

TEST_CASE("WorkerEntity from info", "[WorkerEntity]") {
    // Define a mapping of field names to their specific values
    std::unordered_map<std::string, Json::Value> fieldValues = {
        {"version", 1},
        {"identifier", "cb157baaafe04b988af01a4645d38456"},
        {"Host", "192.168.4.36"},
        {"ContainerHost", "container_host_value"},
        {"RpcPort", 432423},
        {"DataPort", 54237},
        {"SecureRpcPort", 23514},
        {"NettyDataPort", 45837},
        {"WebPort", 65473},
        {"DomainSocketPath", "domain_socket_path_value"},
        {"HttpServerPort", 39282}
    };

    // Dynamically construct worker_info_dict using fieldValues
    Json::Value workerInfoDict;
    workerInfoDict["Identity"]["version"] = fieldValues["version"];
    workerInfoDict["Identity"]["identifier"] = fieldValues["identifier"];

    for (const auto& [key, value] : fieldValues) {
        if (key != "version" && key != "identifier") {
            workerInfoDict["WorkerNetAddress"][key] = value;
        }
    }

    // Convert workerInfoDict to JSON string and then to bytes
    Json::FastWriter writer;
    string workerInfoStr = writer.write(workerInfoDict);

    // Convert worker_info_bytes and instantiate WorkerEntity
    WorkerEntity workerEntity = WorkerEntity::from_worker_info(workerInfoStr);

    // Validate WorkerIdentity fields
    REQUIRE(workerEntity.worker_identity.version == fieldValues["version"].asInt());
    auto identifier = WorkerIdentity::get_bytes_from_hex_string(fieldValues["identifier"].asString());
    REQUIRE(workerEntity.worker_identity.identifier == identifier);

    // Validate WorkerNetAddress fields
    REQUIRE(workerEntity.worker_net_address.host == fieldValues["Host"].asString());
    REQUIRE(workerEntity.worker_net_address.container_host == fieldValues["ContainerHost"].asString());
    REQUIRE(workerEntity.worker_net_address.rpc_port == fieldValues["RpcPort"].asInt());
    REQUIRE(workerEntity.worker_net_address.data_port == fieldValues["DataPort"].asInt());
    REQUIRE(workerEntity.worker_net_address.secure_rpc_port == fieldValues["SecureRpcPort"].asInt());
    REQUIRE(workerEntity.worker_net_address.netty_data_port == fieldValues["NettyDataPort"].asInt());
    REQUIRE(workerEntity.worker_net_address.web_port == fieldValues["WebPort"].asInt());
    REQUIRE(workerEntity.worker_net_address.domain_socket_path == fieldValues["DomainSocketPath"].asString());
    REQUIRE(workerEntity.worker_net_address.http_server_port == fieldValues["HttpServerPort"].asInt());
}


TEST_CASE("Test hash value of WorkerEntity", "[ConsistentHashProvider]") {
    // Create a WorkerIdentity
    const string identifier_hex = "4f23e7a1a230318a912dbdf8f78e0aed";
    auto identifier = WorkerIdentity::get_bytes_from_hex_string(identifier_hex);
    const WorkerIdentity worker_identity(1, identifier);

    // Create a ConsistentHashProvider with default config
    AlluxioClientConfig config;
    ConsistentHashProvider hash_provider(config);

    // Expected hash values
    unordered_set<int32_t> expected_hashes = {-2035703500, -2020503163, -1945615066, -1905409545, 1420996528};

    // Loop over node indices from 0 to 4
    for (int node_index = 0; node_index < DEFAULT_NUM_VIRTUAL_NODES; ++node_index) {
        // Calculate the hash value
        int32_t calculated_hash = hash_provider._hash_worker_identity(worker_identity, node_index);

        // Check if the calculated hash is in the set of expected hashes
        REQUIRE(expected_hashes.find(calculated_hash) != expected_hashes.end());
    }
}