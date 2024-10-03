#ifndef ALLUXIOCLIENT_H
#define ALLUXIOCLIENT_H

#include <string>
#include <vector>
using namespace std;




// Structure to hold worker address information
struct ReadResponse {
    size_t start_offset;             // The starting offset
    size_t bytes;                    // Number of bytes
    std::vector<std::string> IPs;    // List of IP addresses
};

class AlluxioClient {
public:
    // Constructor
    AlluxioClient(const std::string& masterAddress, int port);

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
    std::vector<ReadResponse> getWorkerAddress(
        const std::string& filename,
        size_t offset,
        size_t bytes
    );

private:
    std::string m_masterAddress;  // Alluxio master address
    int m_port;                   // Port number

    // Private helper functions
    std::vector<ReadResponse> queryAlluxioForWorkerAddresses(
        const std::string& filename,
        size_t offset,
        size_t bytes
    );
};

#endif // ALLUXIO_CLIENT_H