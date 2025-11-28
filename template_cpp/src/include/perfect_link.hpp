#pragma once

#include <functional>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <chrono>
#include <netinet/in.h>
#include <cstring>
#include <arpa/inet.h>
#include "message.hpp"
#include "parser.hpp"

class PerfectLink {
public:
    using DeliverCallback = std::function<void(unsigned long from, const Message& msg)>;

    PerfectLink(unsigned long myId, int sockfd, const std::vector<Parser::Host>& hosts, DeliverCallback callback)
        : myId_(myId), sockfd_(sockfd), hosts_(hosts), callback_(callback) {}
    
    // Send a message to a specific process
    void send(unsigned long targetId, const Message& msg) {
        PendingMessage pm;
        pm.msg = msg;
        pm.targetId = targetId;
        pm.lastSendTime = std::chrono::steady_clock::now();
        pm.acked = false;

        // Add to pending list
        pendingMessages_[targetId].push_back(pm);

        // Send immediately
        sendUdp(targetId, msg);
    }
    
    // Handle incoming UDP packet
    void receive(const std::string& data, const struct sockaddr_in& sender_addr) {
        Message msg;
        if (!Message::deserialize(data, msg)) {
            return;
        }

        if (msg.type == MessageType::PL_ACK) {
            // Handle ACK
            auto& pending = pendingMessages_[msg.sender_id];
            for (auto it = pending.begin(); it != pending.end(); ) {
                if (it->msg.seq_no == msg.seq_no && it->msg.original_sender_id == msg.original_sender_id && it->msg.original_seq_no == msg.original_seq_no) {
                    it = pending.erase(it); // Remove acknowledged message
                } else {
                    ++it;
                }
            }
        } else {
            // Handle Data Message
            
            // Send ACK immediately
            Message ack;
            ack.type = MessageType::PL_ACK;
            ack.sender_id = myId_;
            ack.seq_no = msg.seq_no;
            ack.original_sender_id = msg.original_sender_id;
            ack.original_seq_no = msg.original_seq_no;
            ack.payload = "";
            
            sendUdp(msg.sender_id, ack);

            // Deduplicate
            auto key = std::make_pair(msg.sender_id, msg.seq_no);
            if (delivered_.find(key) == delivered_.end()) {
                delivered_.insert(key);
                callback_(msg.sender_id, msg);
            }
        }
    }
    
    // Periodic update for retransmissions
    void update() {
        auto now = std::chrono::steady_clock::now();
        for (auto& [targetId, messages] : pendingMessages_) {
            for (auto& pm : messages) {
                if (std::chrono::duration_cast<std::chrono::milliseconds>(now - pm.lastSendTime).count() > 1000) { // 1 second timeout
                    sendUdp(targetId, pm.msg);
                    pm.lastSendTime = now;
                }
            }
        }
    }

private:
    struct PendingMessage {
        Message msg;
        unsigned long targetId;
        std::chrono::steady_clock::time_point lastSendTime;
        bool acked;
    };

    unsigned long myId_;
    int sockfd_;
    std::vector<Parser::Host> hosts_;
    DeliverCallback callback_;
    
    // Map of targetId -> list of pending messages
    std::map<unsigned long, std::vector<PendingMessage>> pendingMessages_;
    
    // Set of delivered messages (senderId, seqNo) for deduplication
    std::set<std::pair<unsigned long, unsigned long>> delivered_;

    struct sockaddr_in getAddr(unsigned long targetId) {
        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        
        for (const auto& host : hosts_) {
            if (host.id == targetId) {
                addr.sin_addr.s_addr = host.ip;
                addr.sin_port = host.port;
                break;
            }
        }
        return addr;
    }

    void sendUdp(unsigned long targetId, const Message& msg) {
        std::string data = msg.serialize();
        struct sockaddr_in addr = getAddr(targetId);
        
        sendto(sockfd_, data.c_str(), data.size(), 0, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
    }
};
