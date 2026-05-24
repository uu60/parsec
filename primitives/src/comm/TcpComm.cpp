#include "comm/TcpComm.h"

#include "conf/Conf.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <future>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace {
struct MessageHeader {
    int32_t senderRank;
    int32_t tag;
    uint64_t payloadSize;
};

int64_t parseIntParam(const std::string &name, int64_t defaultValue) {
    auto it = Conf::_userParams.find(name);
    if (it == Conf::_userParams.end()) {
        return defaultValue;
    }
    return std::stoll(it->second);
}

std::string parseStringParam(const std::string &name, const std::string &defaultValue) {
    auto it = Conf::_userParams.find(name);
    if (it == Conf::_userParams.end()) {
        return defaultValue;
    }
    return it->second;
}

std::runtime_error socketError(const std::string &message) {
    return std::runtime_error(message + ": " + std::strerror(errno));
}
}

TcpComm::TcpComm() : _peerFds(3, -1), _sendMutexes(3), _recvMutexes(3) {}

TcpComm::~TcpComm() {
    finalize_();
}

int TcpComm::rank_() {
    return _rank;
}

void TcpComm::init_(int argc, char **argv) {
    parseConfig(argc, argv);
    if (_size != 3) {
        throw std::runtime_error("3 parties restricted.");
    }
    establishConnections();
}

void TcpComm::finalize_() {
    closeFd(_listenFd);
    for (auto &fd: _peerFds) {
        closeFd(fd);
    }
}

bool TcpComm::isServer_() {
    return _rank == 0 || _rank == 1;
}

bool TcpComm::isClient_() {
    return !isServer_();
}

void TcpComm::send_(int64_t source, int width, int receiverRank, int tag) {
    sendPayload(receiverRank, tag, encodeInt(source, width));
}

void TcpComm::send_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    sendPayload(receiverRank, tag, encodeVector(source, width));
}

void TcpComm::send_(const std::string &source, int receiverRank, int tag) {
    sendPayload(receiverRank, tag, std::vector<char>(source.begin(), source.end()));
}

void TcpComm::receive_(int64_t &source, int width, int senderRank, int tag) {
    source = decodeInt(receivePayload(senderRank, tag), width);
}

void TcpComm::receive_(std::vector<int64_t> &source, int width, int senderRank, int tag) {
    decodeVector(receivePayload(senderRank, tag), width, source);
}

void TcpComm::receive_(std::string &target, int senderRank, int tag) {
    auto payload = receivePayload(senderRank, tag);
    target.assign(payload.begin(), payload.end());
}

TcpRequestWrapper *TcpComm::sendAsync_(const std::vector<int64_t> &source, int width, int receiverRank, int tag) {
    auto sourceCopy = source;
    return new TcpRequestWrapper(std::async(std::launch::async, [this, sourceCopy = std::move(sourceCopy), width, receiverRank, tag]() {
        send_(sourceCopy, width, receiverRank, tag);
    }));
}

TcpRequestWrapper *TcpComm::sendAsync_(const int64_t &source, int width, int receiverRank, int tag) {
    return new TcpRequestWrapper(std::async(std::launch::async, [this, source, width, receiverRank, tag]() {
        send_(source, width, receiverRank, tag);
    }));
}

TcpRequestWrapper *TcpComm::sendAsync_(const std::string &source, int receiverRank, int tag) {
    auto sourceCopy = source;
    return new TcpRequestWrapper(std::async(std::launch::async, [this, sourceCopy = std::move(sourceCopy), receiverRank, tag]() {
        send_(sourceCopy, receiverRank, tag);
    }));
}

TcpRequestWrapper *TcpComm::receiveAsync_(int64_t &target, int width, int senderRank, int tag) {
    return new TcpRequestWrapper(std::async(std::launch::async, [this, &target, width, senderRank, tag]() {
        receive_(target, width, senderRank, tag);
    }));
}

TcpRequestWrapper *TcpComm::receiveAsync_(std::vector<int64_t> &target, int count, int width, int senderRank, int tag) {
    return new TcpRequestWrapper(std::async(std::launch::async, [this, &target, count, width, senderRank, tag]() {
        receive_(target, width, senderRank, tag);
        if (count >= 0 && static_cast<int>(target.size()) > count) {
            target.resize(count);
        }
    }));
}

TcpRequestWrapper *TcpComm::receiveAsync_(std::string &target, int length, int senderRank, int tag) {
    return new TcpRequestWrapper(std::async(std::launch::async, [this, &target, length, senderRank, tag]() {
        receive_(target, senderRank, tag);
        if (length >= 0 && static_cast<int>(target.size()) > length) {
            target.resize(length);
        }
    }));
}

void TcpComm::parseConfig(int argc, char **argv) {
    (void) argc;
    (void) argv;
    _rank = static_cast<int>(parseIntParam("tcp_rank", 0));
    _size = static_cast<int>(parseIntParam("tcp_size", 3));
    _basePort = static_cast<int>(parseIntParam("tcp_base_port", 18000));
    _host = parseStringParam("tcp_host", "127.0.0.1");

    if (_rank < 0 || _rank >= _size) {
        throw std::runtime_error("tcp_rank must be in [0, tcp_size).");
    }
}

void TcpComm::establishConnections() {
    _listenFd = createListenSocket(_basePort + _rank);

    for (int peer = 0; peer < _rank; ++peer) {
        _peerFds[peer] = connectToPeer(peer);
    }

    for (int peer = _rank + 1; peer < _size; ++peer) {
        int fd = acceptPeer();
        int32_t peerRank = -1;
        readAll(fd, &peerRank, sizeof(peerRank));
        if (peerRank < 0 || peerRank >= _size || peerRank <= _rank) {
            close(fd);
            throw std::runtime_error("Invalid TCP peer rank.");
        }
        _peerFds[peerRank] = fd;
    }
}

int TcpComm::createListenSocket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw socketError("socket failed");
    }

    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0) {
        close(fd);
        throw socketError("setsockopt failed");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        close(fd);
        throw socketError("bind failed");
    }

    if (listen(fd, _size) < 0) {
        close(fd);
        throw socketError("listen failed");
    }

    return fd;
}

int TcpComm::connectToPeer(int peerRank) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw socketError("socket failed");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(_basePort + peerRank));
    if (inet_pton(AF_INET, _host.c_str(), &addr.sin_addr) != 1) {
        close(fd);
        throw std::runtime_error("tcp_host must be an IPv4 address.");
    }

    while (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        if (errno != ECONNREFUSED && errno != ENOENT) {
            close(fd);
            throw socketError("connect failed");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    int32_t rank = _rank;
    writeAll(fd, &rank, sizeof(rank));
    return fd;
}

int TcpComm::acceptPeer() {
    int fd = accept(_listenFd, nullptr, nullptr);
    if (fd < 0) {
        throw socketError("accept failed");
    }
    return fd;
}

void TcpComm::closeFd(int &fd) {
    if (fd >= 0) {
        close(fd);
        fd = -1;
    }
}

void TcpComm::sendPayload(int receiverRank, int tag, const std::vector<char> &payload) {
    if (receiverRank < 0 || receiverRank >= _size || receiverRank == _rank) {
        throw std::runtime_error("Invalid receiver rank.");
    }

    MessageHeader header{static_cast<int32_t>(_rank), static_cast<int32_t>(tag), static_cast<uint64_t>(payload.size())};

    std::lock_guard<std::mutex> lock(_sendMutexes[receiverRank]);
    writeAll(_peerFds[receiverRank], &header, sizeof(header));
    if (!payload.empty()) {
        writeAll(_peerFds[receiverRank], payload.data(), payload.size());
    }
}

std::vector<char> TcpComm::receivePayload(int senderRank, int tag) {
    if (senderRank < 0 || senderRank >= _size || senderRank == _rank) {
        throw std::runtime_error("Invalid sender rank.");
    }

    {
        std::lock_guard<std::mutex> pendingLock(_pendingMutex);
        auto key = std::make_pair(senderRank, tag);
        auto it = _pendingMessages.find(key);
        if (it != _pendingMessages.end() && !it->second.empty()) {
            auto payload = std::move(it->second.back().payload);
            it->second.pop_back();
            if (it->second.empty()) {
                _pendingMessages.erase(it);
            }
            return payload;
        }
    }

    std::lock_guard<std::mutex> recvLock(_recvMutexes[senderRank]);
    while (true) {
        int incomingTag = 0;
        auto payload = readNextMessage(senderRank, incomingTag);
        if (incomingTag == tag) {
            return payload;
        }

        std::lock_guard<std::mutex> pendingLock(_pendingMutex);
        _pendingMessages[std::make_pair(senderRank, incomingTag)].push_back(PendingMessage{std::move(payload)});
    }
}

std::vector<char> TcpComm::readNextMessage(int senderRank, int &tag) {
    MessageHeader header{};
    readAll(_peerFds[senderRank], &header, sizeof(header));
    if (header.senderRank != senderRank) {
        throw std::runtime_error("Unexpected TCP sender rank.");
    }
    tag = header.tag;

    std::vector<char> payload(header.payloadSize);
    if (!payload.empty()) {
        readAll(_peerFds[senderRank], payload.data(), payload.size());
    }
    return payload;
}

void TcpComm::writeAll(int fd, const void *data, size_t length) {
    auto *cursor = static_cast<const char *>(data);
    while (length > 0) {
        ssize_t written = ::send(fd, cursor, length, 0);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw socketError("send failed");
        }
        if (written == 0) {
            throw std::runtime_error("socket closed while sending");
        }
        cursor += written;
        length -= static_cast<size_t>(written);
    }
}

void TcpComm::readAll(int fd, void *data, size_t length) {
    auto *cursor = static_cast<char *>(data);
    while (length > 0) {
        ssize_t received = recv(fd, cursor, length, 0);
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw socketError("recv failed");
        }
        if (received == 0) {
            throw std::runtime_error("socket closed while receiving");
        }
        cursor += received;
        length -= static_cast<size_t>(received);
    }
}

std::vector<char> TcpComm::encodeInt(int64_t value, int width) {
    if (!Conf::ENABLE_TRANSFER_COMPRESSION || width > 32) {
        std::vector<char> payload(sizeof(int64_t));
        auto converted = static_cast<int64_t>(value);
        std::memcpy(payload.data(), &converted, sizeof(converted));
        return payload;
    }
    std::vector<char> payload(elementSize(width));
    if (width == 1) {
        bool converted = static_cast<bool>(value);
        std::memcpy(payload.data(), &converted, sizeof(converted));
    } else if (width <= 8) {
        auto converted = static_cast<int8_t>(value);
        std::memcpy(payload.data(), &converted, sizeof(converted));
    } else if (width <= 16) {
        auto converted = static_cast<int16_t>(value);
        std::memcpy(payload.data(), &converted, sizeof(converted));
    } else {
        auto converted = static_cast<int32_t>(value);
        std::memcpy(payload.data(), &converted, sizeof(converted));
    }
    return payload;
}

std::vector<char> TcpComm::encodeVector(const std::vector<int64_t> &values, int width) {
    size_t itemSize = (!Conf::ENABLE_TRANSFER_COMPRESSION || width > 32) ? sizeof(int64_t) : elementSize(width);
    std::vector<char> payload(values.size() * itemSize);
    char *cursor = payload.data();
    for (auto value: values) {
        auto one = encodeInt(value, width);
        std::memcpy(cursor, one.data(), itemSize);
        cursor += itemSize;
    }
    return payload;
}

int64_t TcpComm::decodeInt(const std::vector<char> &payload, int width) {
    if (!Conf::ENABLE_TRANSFER_COMPRESSION || width > 32) {
        int64_t value = 0;
        std::memcpy(&value, payload.data(), sizeof(value));
        return value;
    }
    if (width == 1) {
        bool value = false;
        std::memcpy(&value, payload.data(), sizeof(value));
        return value;
    }
    if (width <= 8) {
        int8_t value = 0;
        std::memcpy(&value, payload.data(), sizeof(value));
        return value;
    }
    if (width <= 16) {
        int16_t value = 0;
        std::memcpy(&value, payload.data(), sizeof(value));
        return value;
    }
    int32_t value = 0;
    std::memcpy(&value, payload.data(), sizeof(value));
    return value;
}

void TcpComm::decodeVector(const std::vector<char> &payload, int width, std::vector<int64_t> &target) {
    size_t itemSize = (!Conf::ENABLE_TRANSFER_COMPRESSION || width > 32) ? sizeof(int64_t) : elementSize(width);
    if (itemSize == 0 || payload.size() % itemSize != 0) {
        throw std::runtime_error("Invalid TCP vector payload size.");
    }

    target.resize(payload.size() / itemSize);
    for (size_t i = 0; i < target.size(); ++i) {
        std::vector<char> one(itemSize);
        std::memcpy(one.data(), payload.data() + i * itemSize, itemSize);
        target[i] = decodeInt(one, width);
    }
}

size_t TcpComm::elementSize(int width) {
    if (width == 1) {
        return sizeof(bool);
    }
    if (width <= 8) {
        return sizeof(int8_t);
    }
    if (width <= 16) {
        return sizeof(int16_t);
    }
    if (width <= 32) {
        return sizeof(int32_t);
    }
    return sizeof(int64_t);
}
