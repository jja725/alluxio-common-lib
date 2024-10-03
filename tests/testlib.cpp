#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>
#include <alluxio_lib/lib.hpp>
#include <json/json.h>
#include <string>
#include <unordered_map>

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

TEST_CASE("WorkerEntity from info dynamic", "[WorkerEntity]") {
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
    REQUIRE(workerEntity.worker_identity.identifier == fieldValues["identifier"].asString());

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




