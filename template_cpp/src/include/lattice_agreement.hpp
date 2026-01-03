#pragma once

#include "perfect_link.hpp"
#include <set>
#include <map>
#include <vector>
#include <string>
#include <sstream>
#include <algorithm>
#include <iostream>

class LatticeAgreement {
public:
    using DecideCallback = std::function<void(int slot, const std::set<int>& value)>;

    LatticeAgreement(unsigned long myId, PerfectLink& pl, int numProcesses, DecideCallback callback)
        : myId_(myId), pl_(pl), numProcesses_(numProcesses), callback_(callback), pl_seq_(0) {}

    void propose(int slot, const std::set<int>& value) {
        InstanceState& state = instances_[slot];
        if (state.decided) return; // Should not happen if used correctly, but safeguard

        state.active = true;
        state.proposed_value = value;
        state.active_proposal_number++; // Starts at 0, so first is 1
        state.ack_count = 0;
        state.nack_count = 0;
        
        // Broadcast proposal
        // std::cout << "Node " << myId_ << " Proposing slot " << slot << " prop_num " << state.active_proposal_number << " val " << serializeSet(state.proposed_value) << "\n";
        broadcast(slot, MessageType::LA_PROPOSAL, state.active_proposal_number, state.proposed_value);
    }

    void receive(unsigned long from, const Message& msg) {
        // Map message fields back to LA semantics
        // original_sender_id -> slot_number
        // original_seq_no -> proposal_number
        int slot = static_cast<int>(msg.original_sender_id);
        int proposal_number = static_cast<int>(msg.original_seq_no);
        
        InstanceState& state = instances_[slot];
        
        switch (msg.type) {
            case MessageType::LA_PROPOSAL: {
                handleProposal(from, slot, proposal_number, parseSet(msg.payload), state);
                break;
            }
            case MessageType::LA_ACK: {
                handleAck(slot, proposal_number, state);
                break;
            }
            case MessageType::LA_NACK: {
                handleNack(slot, proposal_number, parseSet(msg.payload), state);
                break;
            }
            default:
                break;
        }
    }

private:
    struct InstanceState {
        // Proposer state
        bool active = false;
        size_t ack_count = 0;
        size_t nack_count = 0;
        size_t active_proposal_number = 0;
        std::set<int> proposed_value;
        bool decided = false;
        
        // Acceptor state
        std::set<int> accepted_value;
    };

    unsigned long myId_;
    PerfectLink& pl_;
    int numProcesses_;
    DecideCallback callback_;
    unsigned long pl_seq_;
    
    std::map<int, InstanceState> instances_;

    // Helper: Serialize set to string "1 2 3"
    std::string serializeSet(const std::set<int>& s) {
        std::ostringstream oss;
        bool first = true;
        for (int x : s) {
            if (!first) oss << " ";
            oss << x;
            first = false;
        }
        return oss.str();
    }
    
    // Helper: Deserialize string to set
    std::set<int> parseSet(const std::string& s) {
        std::set<int> res;
        std::istringstream iss(s);
        int val;
        while (iss >> val) {
            res.insert(val);
        }
        return res;
    }

    void broadcast(int slot, MessageType type, size_t proposal_number, const std::set<int>& payloadSet = {}) {
        Message msg;
        msg.type = type;
        msg.original_sender_id = static_cast<unsigned long>(slot);
        msg.original_seq_no = static_cast<unsigned long>(proposal_number);
        msg.payload = serializeSet(payloadSet);
        msg.sender_id = myId_;
        
        for (int i = 1; i <= numProcesses_; ++i) {
            msg.seq_no = ++pl_seq_; // Unique seq for PL
            pl_.send(i, msg);
        }
    }

    void send(unsigned long target, int slot, MessageType type, size_t proposal_number, const std::set<int>& payloadSet = {}) {
        Message msg;
        msg.type = type;
        msg.sender_id = myId_;
        msg.original_sender_id = static_cast<unsigned long>(slot);
        msg.original_seq_no = static_cast<unsigned long>(proposal_number);
        msg.payload = serializeSet(payloadSet);
        msg.seq_no = ++pl_seq_;
        
        pl_.send(target, msg);
    }

    void handleProposal(unsigned long from, int slot, int proposal_number, const std::set<int>& proposed_value, InstanceState& state) {
        // Acceptor Logic
        if (isSubset(state.accepted_value, proposed_value)) {
            state.accepted_value = proposed_value;
            // Send ACK
            send(from, slot, MessageType::LA_ACK, proposal_number);
        } else {
            // Merge and send NACK
            state.accepted_value.insert(proposed_value.begin(), proposed_value.end());
            send(from, slot, MessageType::LA_NACK, proposal_number, state.accepted_value);
        }
    }

    void handleAck(int slot, int proposal_number, InstanceState& state) {
        // Proposer Logic
        if (state.active && static_cast<size_t>(proposal_number) == state.active_proposal_number) {
            state.ack_count++;
            // std::cout << "Node " << myId_ << " Got ACK from ? for slot " << slot << " cnt " << state.ack_count << "\n";
            checkProposerCondition(slot, state);
        }
    }

    void handleNack(int slot, int proposal_number, const std::set<int>& value, InstanceState& state) {
        // Proposer Logic
        if (state.active && static_cast<size_t>(proposal_number) == state.active_proposal_number) {
            state.proposed_value.insert(value.begin(), value.end());
            state.nack_count++;
            // std::cout << "Node " << myId_ << " Got NACK from ? for slot " << slot << " cnt " << state.nack_count << "\n";
            checkProposerCondition(slot, state);
        }
    }

    void checkProposerCondition(int slot, InstanceState& state) {
        if (!state.active) return;
        
        size_t total_responses = state.ack_count + state.nack_count;
        int f = (numProcesses_ - 1) / 2;
        // int majority = f + 1; 
        
        size_t quorum = static_cast<size_t>((numProcesses_ / 2) + 1);

        if (state.nack_count > 0 && total_responses >= quorum) {
            // Majority with at least one NACK -> Retry with updated value
            state.active_proposal_number++;
            state.ack_count = 0;
            state.nack_count = 0;
            // std::cout << "Node " << myId_ << " Retrying slot " << slot << "\n";
            broadcast(slot, MessageType::LA_PROPOSAL, state.active_proposal_number, state.proposed_value);
        } else if (state.ack_count >= quorum) {
            // Majority ACKs -> Decide
            state.decided = true;
            state.active = false;
            // std::cout << "Node " << myId_ << " Decided slot " << slot << "\n";
            callback_(slot, state.proposed_value);
        }
    }

    
    bool isSubset(const std::set<int>& a, const std::set<int>& b) {
        return std::includes(b.begin(), b.end(), a.begin(), a.end());
    }
};
