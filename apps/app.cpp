#include <lib/ConsistentHashProvider.h>
#include <lib/client.h> // Include the header for ConsistentHashProvider
#include <iostream>

int main() {
    try {
        // Create an Alluxio client configuration
        AlluxioClientConfig config(
            "etcd-host:2379",
            "worker-host:30001",
            2379,
            30001,
            120,
            3,
            "default",
            "etcd-username",
            "etcd-password"
        );

        // Create an Alluxio client
        AlluxioClient client("master-host", 29999);

        // Use the client to get worker addresses
        std::vector<ReadResponse> workerAddresses = client.getWorkerAddress("file.txt", 0, 1024);

        // Print the worker addresses
        for (const auto& address : workerAddresses) {
            std::cout << "Start offset: " << address.start_offset << "\n";
            std::cout << "Bytes: " << address.bytes << "\n";
            std::cout << "IPs: ";
            for (const auto& ip : address.IPs) {
                std::cout << ip << " ";
            }
            std::cout << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
    }
    return 0;
}
