#include <chrono>
#include <iostream>
#include <thread>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>

#include "parser.hpp"
#include "hello.h"
#include <signal.h>


static void stop(int) {
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  // write/flush output file if necessary
  std::cout << "Writing output.\n";

  // exit directly from signal handler
  exit(0);
}

int main(int argc, char **argv) {
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  // `true` means that a config file is required.
  // Call with `false` if no config file is necessary.
  bool requireConfig = true;

  Parser parser(argc, argv);
  parser.parse();

  hello();
  std::cout << std::endl;

  std::cout << "My PID: " << getpid() << "\n";
  std::cout << "From a new terminal type `kill -SIGINT " << getpid() << "` or `kill -SIGTERM "
            << getpid() << "` to stop processing packets\n\n";

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

  std::cout << "Doing some initialization...\n\n";

  // Parse config file
  std::ifstream configFile(parser.configPath());
  if (!configFile.is_open()) {
    std::cerr << "Failed to open config file: " << parser.configPath() << std::endl;
    return 1;
  }
  
  int numMessages, targetId;
  configFile >> numMessages >> targetId;
  configFile.close();
  
  std::cout << "Number of messages to send: " << numMessages << "\n";
  std::cout << "Target process ID: " << targetId << "\n\n";

  // Find target host information
  auto allHosts = parser.hosts();
  Parser::Host targetHost;
  bool found = false;
  for (const auto& host : allHosts) {
    if (host.id == static_cast<unsigned long>(targetId)) {
      targetHost = host;
      found = true;
      break;
    }
  }
  
  if (!found) {
    std::cerr << "Target ID " << targetId << " not found in hosts list" << std::endl;
    return 1;
  }

  // Find our own host information
  Parser::Host myHost;
  bool myHostFound = false;
  for (const auto& host : allHosts) {
    if (host.id == parser.id()) {
      myHost = host;
      myHostFound = true;
      break;
    }
  }
  if (!myHostFound) {
    std::cerr << "Could not find our own host with ID " << parser.id() << " in hosts list" << std::endl;
    return 1;
  }

  // Create UDP socket
  int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sockfd < 0) {
    std::cerr << "Error opening socket" << std::endl;
    return 1;
  }

  // Bind to our own port
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

  // Set up target address
  struct sockaddr_in target_addr;
  memset(&target_addr, 0, sizeof(target_addr));
  target_addr.sin_family = AF_INET;
  target_addr.sin_addr.s_addr = targetHost.ip;
  target_addr.sin_port = targetHost.port;

  {
    std::ofstream outputFile(parser.outputPath());
    if (!outputFile.is_open()) {
      std::cerr << "Failed to open output file: " << parser.outputPath() << std::endl;
      close(sockfd);
      return 1;
    }

    std::cout << "Sending " << numMessages << " messages...\n\n";
    for (int i = 1; i <= numMessages; i++) {
      std::string message = std::to_string(i);
      if (sendto(sockfd, message.c_str(), message.length(), 0,
                 reinterpret_cast<struct sockaddr *>(&target_addr), sizeof(target_addr)) < 0) {
        std::cerr << "Error sending message " << i << std::endl;
      }
      outputFile << "b " << i << std::endl;
    }
  }

  std::cout << "Listening for incoming messages...\n\n";

  // Listen for incoming messages
  char buffer[1024];
  struct sockaddr_in sender_addr;
  socklen_t sender_len = sizeof(sender_addr);
  
  while (true) {
    memset(buffer, 0, sizeof(buffer));
    ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0, 
                     reinterpret_cast<struct sockaddr *>(&sender_addr), &sender_len);
    if (n > 0) {
      // Find the sender's process number by comparing IP and port
      long senderId = -1;
      for (const auto& host : allHosts) {
        // Compare the sender's IP and port with each host
        // Both host.port and sender_addr.sin_port are in network byte order
        if (host.ip == sender_addr.sin_addr.s_addr && 
            host.port == sender_addr.sin_port) {
          senderId = static_cast<long>(host.id);
          break;
        }
      }
      
      if (senderId != -1) {
        // Log delivered message with sender's process number and the message content as a string
        std::string messageContent(buffer, n);
        std::ofstream outputFile(parser.outputPath(), std::ios::app);
        if (!outputFile.is_open()) {
          std::cerr << "Failed to open output file for appending: " << parser.outputPath() << std::endl;
          break;
        }
        outputFile << "d " << senderId << " " << messageContent << std::endl;
      } else {
        std::cerr << "Received message from unknown sender" << std::endl;
      }
    }
  }
  close(sockfd);
  return 0;
}
