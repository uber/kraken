# Kraken Multi-Host Deployment

This setup allows deploying Kraken across multiple hosts:
- **Herd**: Central services (proxy, origin, tracker, build-index) on one host
- **Agents**: One agent per host, all using the same port (16000)

## Architecture

```
Host 1 (Herd): 10.0.1.100
├── Proxy: :15000 (push endpoint)
├── Tracker: :15003 (P2P coordination)
├── Origin: :15002 (storage backend)
└── Build Index: :15004 (tag management)

Host 2 (Agent): 10.0.1.101:16000 (pull endpoint)
Host 3 (Agent): 10.0.1.102:16000 (pull endpoint)
Host 4 (Agent): 10.0.1.103:16000 (pull endpoint)
```

P2P distribution happens automatically between agents through the central tracker.

## Quick Start

### 1. Build Images (on herd host)
```bash
# On herd host
make images
```

### 2. Deploy Herd (Host 1)
```bash
# On herd host (e.g., 10.0.1.100)
chmod +x scripts/*.sh
./scripts/deploy_herd.sh 10.0.1.100
```

### 3. Deploy Agents (Other Hosts)
```bash
# On agent host 1 (e.g., 10.0.1.101)
./scripts/deploy_agent.sh 10.0.1.100 10.0.1.101

# On agent host 2 (e.g., 10.0.1.102)
./scripts/deploy_agent.sh 10.0.1.100 10.0.1.102

# On agent host 3 (e.g., 10.0.1.103)
./scripts/deploy_agent.sh 10.0.1.100 10.0.1.103
```

### 4. Test P2P Distribution
```bash
# Push to herd
docker push 10.0.1.100:15000/company/myapp:v1.0

# Pull from agents (same port, different hosts)
chmod +x test/*.sh
./test/kraken-pull.sh company/myapp:v1.0 10.0.1.101:16000
./test/kraken-pull.sh company/myapp:v1.0 10.0.1.102:16000
./test/kraken-pull.sh company/myapp:v1.0 10.0.1.103:16000
```

## Usage Examples

### Local Testing
```bash
# Option 1: Use the deploy script (builds images automatically)
./scripts/deploy_herd.sh localhost

# Option 2: Deploy herd directly (if images already built)
cd examples/multihost  # if not already there
HERD_HOST_IP=localhost ./herd_start_container.sh

# Verify services are running
curl http://localhost:15000/v2/     # Proxy endpoint (should return {})
curl http://localhost:15003/health  # Tracker health check (should return OK)
curl http://localhost:14000/        # TestFS (returns 404, which is normal)

# Deploy local agents (simulating different hosts)
./scripts/deploy_agent.sh localhost localhost

# Test
./test/test_multihost.sh localhost localhost
```

### Production Deployment
```bash
# Deploy herd on central server
./scripts/deploy_herd.sh 10.0.1.100

# Deploy agents on BMS hosts
./scripts/deploy_agent.sh 10.0.1.100 10.0.1.101
./scripts/deploy_agent.sh 10.0.1.100 10.0.1.102
./scripts/deploy_agent.sh 10.0.1.100 10.0.1.103

# Push from CI/CD
docker push 10.0.1.100:15000/company/app:v1.0

# Pull on BMS hosts (P2P distribution)
./test/kraken-pull.sh company/app:v1.0 10.0.1.101:16000
./test/kraken-pull.sh company/app:v1.0 10.0.1.102:16000
./test/kraken-pull.sh company/app:v1.0 10.0.1.103:16000
```

## Key Features

1. **Same Port Across Hosts**: All agents use port 16000 on their respective hosts
2. **Central Coordination**: Herd provides tracker for P2P discovery
3. **Consistent Naming**: `kraken-pull.sh` normalizes image names
4. **Automatic P2P**: Agents discover each other through the tracker
5. **Fallback Support**: Falls back to herd if agent fails

## File Structure

```
examples/multihost/
├── README.md
├── herd_param.sh                  # Herd configuration parameters
├── agent_param.sh                 # Agent configuration parameters
├── herd_start_processes.sh        # Herd startup script
├── herd_start_container.sh        # Herd container launcher
├── agent_start_container.sh       # Agent container launcher
├── config/
│   ├── agent/multihost.yaml       # Agent configuration
│   ├── origin/multihost.yaml      # Origin configuration
│   ├── tracker/multihost.yaml     # Tracker configuration
│   ├── build-index/multihost.yaml # Build index configuration
│   └── proxy/multihost.yaml       # Proxy configuration
├── scripts/
│   ├── deploy_herd.sh             # Deploy herd script
│   └── deploy_agent.sh            # Deploy agent script
└── test/
    ├── test_multihost.sh          # Multi-host test script
    └── kraken-pull.sh             # Image pull with normalization
```

## Environment Variables

- `HERD_HOST_IP`: IP address of the herd host (required)
- `AGENT_HOST_IP`: IP address of the agent host (required for agents)
- `HERD_HOST`: Fallback herd endpoint for kraken-pull.sh (default: localhost:15000)

## Troubleshooting

### Check Service Status
```bash
# On herd host
docker logs kraken-herd-multihost

# On agent hosts
docker logs kraken-agent-$(hostname)
```

### Verify Connectivity
```bash
# Test herd services
curl http://<herd_ip>:15003/health  # Tracker
curl http://<herd_ip>:15000/v2/     # Proxy

# Test agent
curl http://<agent_ip>:16000/v2/    # Agent registry
```

### Clean Up
```bash
# Stop containers
docker stop kraken-herd-multihost
docker stop kraken-agent-$(hostname)

# Remove containers
docker rm kraken-herd-multihost
docker rm kraken-agent-$(hostname)
```
