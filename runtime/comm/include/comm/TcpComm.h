#ifndef TCP_COMM_H
#define TCP_COMM_H

#include "Comm.h"
#include "item/TcpRequestWrapper.h"

#include <cstdint>
#include <condition_variable>
#include <deque>
#include <exception>
#include <map>
#include <mutex>
#include <string>
#include <thread>
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
    std::vector<std::thread> _readerThreads;
    std::mutex _messageMutex;
    std::condition_variable _messageCv;
    bool _shuttingDown{false};
    std::exception_ptr _readerError;
    std::map<std::pair<int, int>, std::deque<PendingMessage>> _pendingMessages;

public:
    TcpComm();

    ~TcpComm() override;

    int rank_() override;

    void init_(int argc, char **argv) override;

    void finalize_() override;

    bool isServer_() override;

    bool isClient_() override;

    void barrier_() override;

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

    void startReaderThreads();

    void readerLoop(int senderRank);

    int createListenSocket(int port);

    int connectToPeer(int peerRank);

    int acceptPeer();

    void closeFd(int &fd);

    void sendPayload(int receiverRank, int tag, const std::vector<char> &payload);

    std::vector<char> receivePayload(int senderRank, int tag);

    bool readNextMessage(int senderRank, int &tag, std::vector<char> &payload);

    static void writeAll(int fd, const void *data, size_t length);

    static bool readAll(int fd, void *data, size_t length);

    static std::vector<char> encodeInt(int64_t value, int width);

    static std::vector<char> encodeVector(const std::vector<int64_t> &values, int width);

    static int64_t decodeInt(const std::vector<char> &payload, int width);

    static void decodeVector(const std::vector<char> &payload, int width, std::vector<int64_t> &target);

    static size_t elementSize(int width);
};

#endif
