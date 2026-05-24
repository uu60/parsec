#ifndef TCP_COMM_H
#define TCP_COMM_H

#include "Comm.h"
#include "item/TcpRequestWrapper.h"

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

class TcpComm : public Comm {
private:
    struct PendingMessage {
        std::vector<char> payload;
    };

    int _rank{};
    int _size{3};
    int _basePort{18000};
    std::string _host{"127.0.0.1"};
    int _listenFd{-1};
    std::vector<int> _peerFds;
    std::vector<std::mutex> _sendMutexes;
    std::vector<std::mutex> _recvMutexes;
    std::mutex _pendingMutex;
    std::map<std::pair<int, int>, std::vector<PendingMessage>> _pendingMessages;

public:
    TcpComm();

    ~TcpComm() override;

    int rank_() override;

    void init_(int argc, char **argv) override;

    void finalize_() override;

    bool isServer_() override;

    bool isClient_() override;

    void send_(int64_t source, int width, int receiverRank, int tag) override;

    void send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) override;

    void send_(const std::string &source, int receiverRank, int tag) override;

    void receive_(int64_t &source, int width, int senderRank, int tag) override;

    void receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) override;

    void receive_(std::string &target, int senderRank, int tag) override;

    TcpRequestWrapper *sendAsync_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) override;

    TcpRequestWrapper *sendAsync_(const int64_t &source, int width, int receiverRank, int tag) override;

    TcpRequestWrapper *sendAsync_(const std::string &source, int receiverRank, int tag) override;

    TcpRequestWrapper *receiveAsync_(int64_t &target, int width, int senderRank, int tag) override;

    TcpRequestWrapper *receiveAsync_(std::vector<int64_t> &target, int count, int width, int senderRank, int tag) override;

    TcpRequestWrapper *receiveAsync_(std::string &target, int length, int senderRank, int tag) override;

private:
    void parseConfig(int argc, char **argv);

    void establishConnections();

    int createListenSocket(int port);

    int connectToPeer(int peerRank);

    int acceptPeer();

    void closeFd(int &fd);

    void sendPayload(int receiverRank, int tag, const std::vector<char> &payload);

    std::vector<char> receivePayload(int senderRank, int tag);

    std::vector<char> readNextMessage(int senderRank, int &tag);

    static void writeAll(int fd, const void *data, size_t length);

    static void readAll(int fd, void *data, size_t length);

    static std::vector<char> encodeInt(int64_t value, int width);

    static std::vector<char> encodeVector(const std::vector<int64_t> &values, int width);

    static int64_t decodeInt(const std::vector<char> &payload, int width);

    static void decodeVector(const std::vector<char> &payload, int width, std::vector<int64_t> &target);

    static size_t elementSize(int width);
};

#endif
