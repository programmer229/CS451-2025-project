#pragma once

#include <string>
#include <vector>
#include <sstream>
#include <iostream>

enum class MessageType {
    PL_ACK,
    URB_MSG,
    LA_PROPOSAL,
    LA_ACK,
    LA_NACK
};

struct Message {
    MessageType type;
    unsigned long sender_id;
    unsigned long seq_no;
    unsigned long original_sender_id; // For URB
    unsigned long original_seq_no;    // For URB
    std::string payload;

    // Serialize message to string
    // Format: TYPE SENDER_ID SEQ_NO ORIG_SENDER ORIG_SEQ PAYLOAD
    std::string serialize() const {
        std::ostringstream oss;
        oss << static_cast<int>(type) << " " 
            << sender_id << " " 
            << seq_no << " "
            << original_sender_id << " "
            << original_seq_no << " "
            << payload;
        return oss.str();
    }

    // Deserialize string to message
    static bool deserialize(const std::string& data, Message& msg) {
        std::istringstream iss(data);
        int typeInt;
        if (!(iss >> typeInt >> msg.sender_id >> msg.seq_no >> msg.original_sender_id >> msg.original_seq_no)) {
            return false;
        }
        msg.type = static_cast<MessageType>(typeInt);
        
        // Read remaining payload (handling spaces)
        char space;
        iss.get(space); // Consume the space after the last number
        std::getline(iss, msg.payload);
        return true;
    }
};
