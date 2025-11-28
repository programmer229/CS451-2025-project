#pragma once

#include "perfect_link.hpp"
#include <set>
#include <map>
#include <iostream>

class UniformReliableBroadcast {
public:
    using DeliverCallback = std::function<void(unsigned long from, const Message& msg)>;

    UniformReliableBroadcast(unsigned long myId, PerfectLink& pl, int numProcesses, DeliverCallback callback)
        : myId_(myId), pl_(pl), numProcesses_(numProcesses), callback_(callback), pl_seq_(0) {}

    void broadcast(const Message& msg) {
        std::pair<unsigned long, unsigned long> msgId = {msg.original_sender_id, msg.original_seq_no};
        
        if (forwarded_.find(msgId) == forwarded_.end()) {
            pending_[msgId] = msg;
            forwarded_.insert(msgId);
            acks_[msgId].insert(myId_); // We have seen it
            
            for (int i = 1; i <= numProcesses_; ++i) {
                    Message toSend = msg;
                    toSend.sender_id = myId_;
                    toSend.seq_no = ++pl_seq_;
                    
                    pl_.send(i, toSend);
            }
        }
    }

    void deliver(unsigned long from, const Message& msg) {
        std::pair<unsigned long, unsigned long> msgId = {msg.original_sender_id, msg.original_seq_no};
        
        acks_[msgId].insert(from);
        acks_[msgId].insert(myId_);
        
        if (pending_.find(msgId) == pending_.end()) {
            pending_[msgId] = msg;
        }

        if (forwarded_.find(msgId) == forwarded_.end()) {
            forwarded_.insert(msgId);
            
            for (int i = 1; i <= numProcesses_; ++i) {
                    Message toSend = msg;
                    toSend.sender_id = myId_;
                    toSend.seq_no = ++pl_seq_;
                    pl_.send(i, toSend);
            }
        }
        
        // std::cout << "URB: acks for " << msg.original_sender_id << ":" << msg.original_seq_no << " are " << acks_[msgId].size() << "\n";
        
        if (canDeliver(msgId) && delivered_.find(msgId) == delivered_.end()) {
            delivered_.insert(msgId);
            callback_(msg.original_sender_id, msg);
        }
    }

private:
    unsigned long myId_;
    PerfectLink& pl_;
    int numProcesses_;
    DeliverCallback callback_;
    unsigned long pl_seq_;
    
    // Map of (sender, seq) -> Message
    std::map<std::pair<unsigned long, unsigned long>, Message> pending_;
    
    // Set of forwarded messages (sender, seq)
    std::set<std::pair<unsigned long, unsigned long>> forwarded_;
    
    // Map of (sender, seq) -> Set of ACKs (process IDs)
    std::map<std::pair<unsigned long, unsigned long>, std::set<unsigned long>> acks_;
    
    // Set of delivered messages (sender, seq)
    std::set<std::pair<unsigned long, unsigned long>> delivered_;

    bool canDeliver(const std::pair<unsigned long, unsigned long>& msgId) {
        return acks_[msgId].size() > static_cast<size_t>(numProcesses_ / 2);
    }
};
