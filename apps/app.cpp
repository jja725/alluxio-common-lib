#include <iostream>
#include <alluxio_lib/lib.hpp>

int main() {
    // ensure you have an etcd server running on localhost:2379 and alluxio workers registered
    alluxio::AlluxioClientConfig config;
    config.etcd_urls = "http://localhost:2379";
    alluxio::AlluxioClient alluxio_client(config);
    for (int i = 0; i < 10; ++i) {
        auto response = alluxio_client.getWorkerAddress("s3://bucket/path"+i,0,10);
        for (auto read_location: response) {
            for (auto worker: read_location.workers) {
                std::cout << "host: " << worker.host << std::endl;
            }
        }
    }
}
