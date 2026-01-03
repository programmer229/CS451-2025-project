#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <set>
#include <optional>
#include <cerrno>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <sys/time.h>
#include <sys/select.h>
#include <signal.h>

#include "parser.hpp"
#include "hello.h"
#include "perfect_link.hpp"
#include "urb.hpp"
#include "fifo_broadcast.hpp"
#include "lattice_agreement.hpp"

static std::ofstream outputFile;

static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  // write/flush output file if necessary
  std::cout << "Writing output.\n";
  if (outputFile.is_open()) {
    outputFile.flush();
    outputFile.close();
  }

  // exit directly from signal handler
  exit(0);
}

int main(int argc, char **argv) {
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  Parser parser(argc, argv);
  parser.parse();

  hello();
  std::cout << std::endl;

  std::cout << "My PID: " << getpid() << "\n";
  std::cout << "My ID: " << parser.id() << "\n\n";

  auto hosts = parser.hosts();
  
  // Parse config file to determine mode
  std::ifstream configFile(parser.configPath());
  if (!configFile.is_open()) {
    std::cerr << "Failed to open config file: " << parser.configPath() << std::endl;
    return 1;
  }
  
  std::string line;
  if (!std::getline(configFile, line)) {
      std::cerr << "Empty config file" << std::endl;
      return 1;
  }
  
  std::stringstream ss(line);
  int val;
  std::vector<int> configTokens;
  while(ss >> val) configTokens.push_back(val);
  
  bool isLatticeAgreement = (configTokens.size() >= 3);
  int numMessagesOrProposals = configTokens.empty() ? 0 : configTokens[0];
  
  std::cout << "Config Mode: " << (isLatticeAgreement ? "Lattice Agreement" : "FIFO/PL") << "\n";
  std::cout << "Count: " << numMessagesOrProposals << "\n";
  std::cout << "Hosts count: " << hosts.size() << "\n\n";

  // Create UDP socket
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    std::cerr << "Error opening socket" << std::endl;
    return 1;
  }
  
  int opt = 1;
  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
      perror("setsockopt");
      return 1;
  }

  // Bind to our own port
  Parser::Host myHost;
  bool myHostFound = false;
  for (const auto& host : hosts) {
    if (host.id == parser.id()) {
      myHost = host;
      myHostFound = true;
      break;
    }
  }
  
  if (!myHostFound) {
      std::cerr << "My host not found!" << std::endl;
      return 1;
  }

  struct sockaddr_in my_addr;
  memset(&my_addr, 0, sizeof(my_addr));
  my_addr.sin_family = AF_INET;
  my_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  my_addr.sin_port = myHost.port;
  
  if (bind(sockfd, reinterpret_cast<struct sockaddr *>(&my_addr), sizeof(my_addr)) < 0) {
    std::cerr << "Error binding socket" << std::endl;
    close(sockfd);
    return 1;
  }

  // Open output file
  std::cout << "Opening output file: " << parser.outputPath() << "\n";
  outputFile.open(parser.outputPath());
  if (!outputFile.is_open()) {
      std::cerr << "Failed to open output file" << std::endl;
      return 1;
  }

  // Networking buffers
  char buffer[65536];
  struct sockaddr_in sender_addr;
  socklen_t sender_len = sizeof(sender_addr);
  
  if (isLatticeAgreement) {
      // --- Milestone 3: Lattice Agreement ---
      
      // Parse Proposals
      std::vector<std::set<int>> proposals;
      for (int i = 0; i < numMessagesOrProposals; ++i) {
          if (std::getline(configFile, line)) {
              std::set<int> p;
              std::stringstream pss(line);
              int v;
              while (pss >> v) p.insert(v);
              proposals.push_back(p);
          }
      }
      configFile.close();
      
      // Output Ordering Logic
      std::map<int, std::set<int>> pendingDecisions;
      int nextSlotToPrint = 0;
      
      auto decideCallback = [&](int slot, const std::set<int>& value) {
           pendingDecisions[slot] = value;
           while (pendingDecisions.count(nextSlotToPrint)) {
               const auto& s = pendingDecisions[nextSlotToPrint];
               bool first = true;
               for (int x : s) {
                   if (!first) outputFile << " ";
                   outputFile << x;
                   first = false;
               }
               outputFile << "\n";
               outputFile.flush(); // Keep flush for safety
               nextSlotToPrint++;
           }
      };
      
      // Setup Layers
      LatticeAgreement* laPtr = nullptr;
      
      auto plDeliver = [&](unsigned long from, const Message& msg) {
          // Only route LA messages to LatticeAgreement
          if (msg.type == MessageType::LA_PROPOSAL || 
              msg.type == MessageType::LA_ACK || 
              msg.type == MessageType::LA_NACK) {
              if (laPtr) laPtr->receive(from, msg);
          }
      };
      
      PerfectLink pl(parser.id(), sockfd, hosts, plDeliver);
      LatticeAgreement la(parser.id(), pl, static_cast<int>(hosts.size()), decideCallback);
      laPtr = &la;
      
      // Start Agreement for all slots
      for (int i = 0; i < static_cast<int>(proposals.size()); ++i) {
          la.propose(i, proposals[i]);
      }
      
      // Event Loop
      // Continue processing network messages even after deciding all slots
      // so we can help other nodes catch up.
      while (true) {
          fd_set readfds;
          FD_ZERO(&readfds);
          FD_SET(sockfd, &readfds);
          
          struct timeval tv;
          tv.tv_sec = 0;
          tv.tv_usec = 1000; // 1ms
          
          int ready = select(sockfd + 1, &readfds, nullptr, nullptr, &tv);
          
          if (ready > 0 && FD_ISSET(sockfd, &readfds)) {
              memset(buffer, 0, sizeof(buffer));
              ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, 
                               reinterpret_cast<struct sockaddr *>(&sender_addr), &sender_len);
              if (n > 0) {
                  std::string data(buffer, n);
                  pl.receive(data, sender_addr);
              }
          }
          
          pl.update();
          
          // Optional: Break if signal received (handled by signal handler anyway)
      }
      
  } else {
      // --- Milestone 1 & 2: Perfect Links / FIFO ---
      configFile.close();
      
      // FIFO Callback
      auto fifoDeliver = [&](unsigned long from, const Message& msg) {
          outputFile << "d " << from << " " << msg.payload << "\n"; 
      };

      UniformReliableBroadcast* urbPtr = nullptr;
      FIFOBroadcast* fifoPtr = nullptr;
      
      auto plDeliver = [&](unsigned long from, const Message& msg) {
          if (msg.type == MessageType::URB_MSG) {
              if (urbPtr) urbPtr->deliver(from, msg);
          }
      };
      
      PerfectLink pl(parser.id(), sockfd, hosts, plDeliver);
      
      auto urbDeliver = [&](unsigned long from, const Message& msg) {
          if (fifoPtr) fifoPtr->deliver(from, msg);
      };
      
      UniformReliableBroadcast urb(parser.id(), pl, static_cast<int>(hosts.size()), urbDeliver);
      urbPtr = &urb;
      
      FIFOBroadcast fifo(parser.id(), urb, fifoDeliver);
      fifoPtr = &fifo;

      // Broadcast loop
      std::cout << "Broadcasting " << numMessagesOrProposals << " messages...\n";
      
      for (int i = 1; i <= numMessagesOrProposals; ++i) {
          Message msg;
          msg.type = MessageType::URB_MSG;
          msg.payload = std::to_string(i);
          
          fifo.broadcast(msg);
          outputFile << "b " << i << "\n";
          
          // Drain queue
          while (true) {
              fd_set readfds;
              FD_ZERO(&readfds);
              FD_SET(sockfd, &readfds);
              
              struct timeval tv;
              tv.tv_sec = 0;
              tv.tv_usec = 0;
              
              int ready = select(sockfd + 1, &readfds, nullptr, nullptr, &tv);
              
              if (ready > 0 && FD_ISSET(sockfd, &readfds)) {
                   memset(buffer, 0, sizeof(buffer));
                   ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, 
                                    reinterpret_cast<struct sockaddr *>(&sender_addr), &sender_len);
                   if (n > 0) {
                       std::string data(buffer, n);
                       pl.receive(data, sender_addr);
                   }
              } else {
                  break;
              }
          }
          pl.update();
      }
      
      // Final event loop
      while (true) {
          fd_set readfds;
          FD_ZERO(&readfds);
          FD_SET(sockfd, &readfds);
          
          struct timeval tv;
          tv.tv_sec = 0;
          tv.tv_usec = 10000; // 10ms
          
          int ready = select(sockfd + 1, &readfds, nullptr, nullptr, &tv);
          
          if (ready > 0 && FD_ISSET(sockfd, &readfds)) {
              memset(buffer, 0, sizeof(buffer));
              ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, 
                               reinterpret_cast<struct sockaddr *>(&sender_addr), &sender_len);
              if (n > 0) {
                  std::string data(buffer, n);
                  pl.receive(data, sender_addr);
              }
          }
          pl.update();
      }
  }

  std::cout << "Done deciding. Waiting for termination...\n";
  while (true) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return 0;
}
