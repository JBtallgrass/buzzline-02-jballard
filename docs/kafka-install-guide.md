# Kafka Installation Guide - WSL Ubuntu

## System Requirements
- Windows 10 version 2004 or higher
- Administrator access
- 4GB RAM minimum
- 4GB available storage

## Install WSL & Ubuntu
In PowerShell (Administrator):
```powershell
wsl --install -d Ubuntu
```

## Verify WSL Installation
```powershell
wsl -l -v  # Should show VERSION 2
wsl        # Enter Ubuntu environment
```

## Install Latest Java Development Kit (JDK)
```bash
sudo apt update
sudo apt install default-jdk  # Installs latest version

# Example for specific version (e.g., JDK 17):
# sudo apt install openjdk-17-jdk

# Verify Java version
java --version

# Switch Java version if needed:
sudo update-alternatives --config java
```

## Install Apache Kafka
```bash
cd ~

# Get latest version from https://kafka.apache.org/downloads
wget https://downloads.apache.org/kafka/[VERSION]/kafka_2.13-[VERSION].tgz

# Example (Version 3.9.0):
# wget https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz

# Extract and rename
tar -xvzf kafka_*.tgz
mv kafka_* kafka
```

## Start Kafka Services
### Terminal 1 - Start Zookeeper
```bash
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Terminal 2 - Start Kafka Server
```bash
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

### Terminal 3 - Create and Verify Topic
```bash
cd ~/kafka
# Create test topic
bin/kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Verify topic creation
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

## Troubleshooting
1. WSL Installation:
   - Enable virtualization in BIOS
   - Run Windows Update
   - Verify system requirements

2. Java Version:
   - Use `sudo update-alternatives --config java`
   - Select desired version

3. Kafka Services:
   - Verify Java installation
   - Check port 9092
   - Ensure Zookeeper runs first

## Additional Resources
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [WSL Documentation](https://learn.microsoft.com/en-us/windows/wsl/install)
- [OpenJDK Documentation](https://openjdk.org/)
