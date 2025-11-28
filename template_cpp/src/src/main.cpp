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

static bool running = true;

static void stop(int) {
  running = false;
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

  std::cout << "List of resolved hosts is:\n";
  std::cout << "==========================\n";
  auto hosts = parser.hosts();
  for (auto &host : hosts) {
    std::cout << host.id << "\n";
    std::cout << "Human-readable IP: " << host.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << host.ip << "\n";
    std::cout << "Human-readbale Port: " << host.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << host.port << "\n";
    std::cout << "\n";
  }
  std::cout << "\n";

  std::cout << "Path to output:\n";
  std::cout << "===============\n";
  std::cout << parser.outputPath() << "\n\n";

  std::cout << "Path to config:\n";
  std::cout << "===============\n";
  std::cout << parser.configPath() << "\n\n";

  // Parse config file
  std::ifstream configFile(parser.configPath());
  if (!configFile.is_open()) {
    std::cerr << "Failed to open config file: " << parser.configPath() << std::endl;
    return 1;
  }
  
  int numMessages;
  configFile >> numMessages;
  configFile.close();
  
  std::cout << "Number of messages to broadcast: " << numMessages << "\n\n";

  // Create UDP socket
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    std::cerr << "Error opening socket" << std::endl;
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
  std::ofstream outputFile(parser.outputPath());
  if (!outputFile.is_open()) {
      std::cerr << "Failed to open output file" << std::endl;
      return 1;
  }

  // Instantiate layers
  // Forward declarations for callbacks
  
  // FIFO Callback: Write to log
  auto fifoDeliver = [&](unsigned long from, const Message& msg) {
      outputFile << "d " << from << " " << msg.payload << "\n"; // payload is the sequence number
      // Flush periodically or let OS handle it? 
      // For correctness in crash scenarios, we might want to flush often, 
      // but for performance we shouldn't flush every line.
      // The instructions say: "You may consider more sophisticated logging approaches... 
      // writing to files is the only action we allow a process to do after receiving a SIGINT or SIGTERM signal."
      // So we can rely on the OS buffer and flush on exit.
  };

  // URB Callback: Pass to FIFO
  // We need to construct objects first.
  
  // We need to break the dependency cycle or use references.
  // PL -> URB -> FIFO
  // PL callback calls URB.deliver
  // URB callback calls FIFO.deliver
  
  // But objects need to be constructed.
  
  // PerfectLink needs a callback.
  // URB needs PerfectLink and a callback.
  // FIFO needs URB and a callback.
  
  // We can use std::function and bind/lambda to defer resolution.
  
  UniformReliableBroadcast* urbPtr = nullptr;
  FIFOBroadcast* fifoPtr = nullptr;
  
  auto plDeliver = [&](unsigned long from, const Message& msg) {
      if (urbPtr) urbPtr->deliver(from, msg);
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
  std::cout << "Broadcasting " << numMessages << " messages...\n";
  
  for (int i = 1; i <= numMessages; ++i) {
      Message msg;
      msg.type = MessageType::URB_MSG;
      msg.payload = std::to_string(i);
      // sender/seq set by FIFO
      
      fifo.broadcast(msg);
      outputFile << "b " << i << "\n";
  }

  // Event loop
  char buffer[65536];
  struct sockaddr_in sender_addr;
  socklen_t sender_len = sizeof(sender_addr);

  while (running) {
      fd_set readfds;
      FD_ZERO(&readfds);
      FD_SET(sockfd, &readfds);
      
      struct timeval tv;
      tv.tv_sec = 0;
      tv.tv_usec = 10000; // 10ms timeout for update loop
      
      int ready = select(sockfd + 1, &readfds, nullptr, nullptr, &tv);
      
      if (ready > 0) {
          if (FD_ISSET(sockfd, &readfds)) {
              memset(buffer, 0, sizeof(buffer));
              ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, 
                               reinterpret_cast<struct sockaddr *>(&sender_addr), &sender_len);
              if (n > 0) {
                  std::string data(buffer, n);
                  pl.receive(data, sender_addr);
              }
          }
      }
      
      pl.update();
  }

  std::cout << "Stopping...\n";
  outputFile.flush();
  outputFile.close();
  close(sockfd);

  return 0;
}
