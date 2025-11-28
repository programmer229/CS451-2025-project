#pragma once

#include "urb.hpp"
#include <map>
#include <iostream>

class FIFOBroadcast {
public:
    using DeliverCallback = std::function<void(unsigned long from, const Message& msg)>;

    FIFOBroadcast(unsigned long myId, UniformReliableBroadcast& urb, DeliverCallback callback)
        : myId_(myId), urb_(urb), callback_(callback), mySeq_(0) {}

    void broadcast(const Message& msg) {
        Message taggedMsg = msg;
        taggedMsg.original_sender_id = myId_;
        taggedMsg.original_seq_no = ++mySeq_;
        
        urb_.broadcast(taggedMsg);
    }

    void deliver(unsigned long from, const Message& msg) {
        unsigned long sender = msg.original_sender_id;
        unsigned long seq = msg.original_seq_no;
        
        if (nextSeq_.find(sender) == nextSeq_.end()) {
            nextSeq_[sender] = 1;
        }
        
        buffer_[sender][seq] = msg;
        
        while (buffer_[sender].count(nextSeq_[sender])) {
            Message nextMsg = buffer_[sender][nextSeq_[sender]];
            buffer_[sender].erase(nextSeq_[sender]);
            
            callback_(sender, nextMsg);
            
            nextSeq_[sender]++;
        }
    }

private:
    unsigned long myId_;
    UniformReliableBroadcast& urb_;
    DeliverCallback callback_;

    std::map<unsigned long, unsigned long> nextSeq_;
    std::map<unsigned long, std::map<unsigned long, Message>> buffer_;
    unsigned long mySeq_;
};
