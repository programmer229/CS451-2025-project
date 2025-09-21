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

  // Track delivered messages to provide at-most-once semantics when retrying.
  std::set<std::pair<long, long>> deliveredMessages;

  auto handleIncoming = [&](const std::string &incomingMessage,
                            const struct sockaddr_in &sender_addr)
      -> std::optional<std::pair<long, long>> {
    long senderId = -1;
    for (const auto &host : allHosts) {
      if (host.ip == sender_addr.sin_addr.s_addr &&
          host.port == sender_addr.sin_port) {
        senderId = static_cast<long>(host.id);
        break;
      }
    }

    if (senderId == -1) {
      std::cerr << "Received message from unknown sender" << std::endl;
      return std::nullopt;
    }

    std::istringstream iss(incomingMessage);
    std::string type;
    if (!(iss >> type)) {
      std::cerr << "Received malformed message" << std::endl;
      return std::nullopt;
    }

    if (type == "MSG") {
      long sequence;
      if (!(iss >> sequence)) {
        std::cerr << "Received malformed data message" << std::endl;
        return std::nullopt;
      }

      std::string payload;
      std::getline(iss, payload);
      if (!payload.empty() && payload.front() == ' ') {
        payload.erase(payload.begin());
      }

      std::ostringstream ackStream;
      ackStream << "ACK " << sequence;
      const std::string ackPayload = ackStream.str();
      if (sendto(sockfd, ackPayload.c_str(), ackPayload.size(), 0,
                 reinterpret_cast<const struct sockaddr *>(&sender_addr),
                 sizeof(sender_addr)) < 0) {
        std::cerr << "Error sending ACK for sequence " << sequence << std::endl;
      }

      auto key = std::make_pair(senderId, sequence);
      if (deliveredMessages.insert(key).second) {
        std::ofstream outputFile(parser.outputPath(), std::ios::app);
        if (!outputFile.is_open()) {
          std::cerr << "Failed to open output file for appending: "
                    << parser.outputPath() << std::endl;
        } else {
          outputFile << "d " << senderId << " " << payload << std::endl;
        }
      }
    } else if (type == "ACK") {
      long sequence;
      if (!(iss >> sequence)) {
        std::cerr << "Received malformed ACK message" << std::endl;
        return std::nullopt;
      }
      return std::make_pair(senderId, sequence);
    } else {
      std::cerr << "Received unsupported message type: " << type << std::endl;
    }

    return std::nullopt;
  };

  if (targetHost.id == parser.id()) {
    std::cout << "Configured target is this process; skipping send phase.\n\n";
  } else {
    std::ofstream outputFile(parser.outputPath());
    if (!outputFile.is_open()) {
      std::cerr << "Failed to open output file: " << parser.outputPath() << std::endl;
      close(sockfd);
      return 1;
    }

    std::cout << "Sending " << numMessages << " messages...\n\n";
    const auto resendInterval = std::chrono::milliseconds(100);
    char buffer[1024];

    for (int i = 1; i <= numMessages; i++) {
      std::string payload = std::to_string(i);
      outputFile << "b " << i << std::endl;

      bool acked = false;
      bool messageSent = false;
      auto lastSendTime = std::chrono::steady_clock::now();

      while (!acked) {
        if (!messageSent) {
          std::ostringstream wireStream;
          wireStream << "MSG " << i << " " << payload;
          const std::string wireMessage = wireStream.str();
          if (sendto(sockfd, wireMessage.c_str(), wireMessage.size(), 0,
                     reinterpret_cast<struct sockaddr *>(&target_addr),
                     sizeof(target_addr)) < 0) {
            std::cerr << "Error sending message " << i << std::endl;
          }
          messageSent = true;
          lastSendTime = std::chrono::steady_clock::now();
        }

        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(sockfd, &readfds);

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - lastSendTime);
        auto remaining = resendInterval - elapsed;
        if (remaining < std::chrono::milliseconds::zero()) {
          remaining = std::chrono::milliseconds::zero();
        }

        struct timeval tv;
        tv.tv_sec = static_cast<long>(remaining.count() / 1000);
        tv.tv_usec = static_cast<long>((remaining.count() % 1000) * 1000);

        int ready = select(sockfd + 1, &readfds, nullptr, nullptr, &tv);
        if (ready < 0) {
          if (errno == EINTR) {
            continue;
          }
          std::cerr << "Error during select" << std::endl;
          continue;
        } else if (ready == 0) {
          messageSent = false;
          continue;
        }

        if (FD_ISSET(sockfd, &readfds)) {
          memset(buffer, 0, sizeof(buffer));
          struct sockaddr_in sender_addr;
          socklen_t sender_len = sizeof(sender_addr);
          ssize_t n = recvfrom(sockfd, buffer, sizeof(buffer) - 1, 0,
                               reinterpret_cast<struct sockaddr *>(&sender_addr),
                               &sender_len);
          if (n > 0) {
            std::string incoming(buffer, n);
            auto ackInfo = handleIncoming(incoming, sender_addr);
            if (ackInfo.has_value() &&
                ackInfo->first == static_cast<long>(targetHost.id) &&
                ackInfo->second == i) {
              acked = true;
            }
          }
        }

        if (!acked &&
            std::chrono::steady_clock::now() - lastSendTime >= resendInterval) {
          messageSent = false;
        }
      }
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
      std::string incoming(buffer, n);
      handleIncoming(incoming, sender_addr);
    }
  }
  close(sockfd);
  return 0;
}
