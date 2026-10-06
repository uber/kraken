# Kraken P2P Docker Registry - Mac Setup Codelab

This codelab will guide you through setting up and testing Kraken, a P2P-powered Docker registry, on your Mac using Docker containers.

## What is Kraken?

Kraken is a P2P powered Docker registry that focuses on scalability and availability. It distributes Docker images using peer-to-peer technology, reducing the load on central servers and improving download speeds, especially for large deployments.

### Key Components:
- **Agent**: Deployed on each host, implements Docker registry interface, handles P2P distribution
- **Origin**: Dedicated seeders that store blobs backed by pluggable storage (S3, GCS, etc.)
- **Tracker**: Tracks which peers have what content and orchestrates P2P connections
- **Proxy**: Implements Docker registry interface for uploads, distributes to origins
- **Build-Index**: Maps human-readable tags to blob digests

## Prerequisites

Before starting, ensure you have:
- **Docker Desktop for Mac** (required for `host.docker.internal` networking)
- **Go** (for building binaries)
- **Make** (for build automation)
- **Git** (to clone the repository)

## Step 1: Clone and Setup

```bash
# Clone the Kraken repository
git clone https://github.com/uber/kraken.git
cd kraken
```

## Step 2: Build and Start Kraken

Let's build and start Kraken:

```bash
# Build binaries and start the development cluster
make devcluster
```

This command will:
1. Build Linux binaries using Docker containers
2. Build Docker images for all Kraken components
3. Start 3 containers:
   - `kraken-herd`: Contains origin, tracker, build-index, proxy, and testfs
   - `kraken-agent-one`: First agent instance
   - `kraken-agent-two`: Second agent instance

## Step 3: Verify the Setup

Check that all containers are running:

```bash
docker ps --filter name=kraken
```

**Expected Output:**
```
CONTAINER ID   IMAGE              COMMAND                  CREATED         STATUS         PORTS                                                                                                                  NAMES
295abac48744   kraken-agent:dev   "/usr/bin/kraken-age…"   3 minutes ago   Up 3 minutes   0.0.0.0:17000-17002->17000-17002/tcp, :::17000-17002->17000-17002/tcp                                                  kraken-agent-two
7159ab30731d   kraken-agent:dev   "/usr/bin/kraken-age…"   3 minutes ago   Up 3 minutes   0.0.0.0:16000-16002->16000-16002/tcp, :::16000-16002->16000-16002/tcp                                                  kraken-agent-one
12560448ad3b   kraken-herd:dev    "./herd_start_proces…"   3 minutes ago   Up 3 minutes   0.0.0.0:14000->14000/tcp, :::14000->14000/tcp, 0.0.0.0:15000-15005->15000-15005/tcp, :::15000-15005->15000-15005/tcp   kraken-herd
```

You should see **three containers running** with **STATUS = "Up"**:
- **`kraken-herd`**: Central services (proxy, origin, tracker, build-index, testfs)
- **`kraken-agent-one`**: First agent instance (ports 16000-16002)
- **`kraken-agent-two`**: Second agent instance (ports 17000-17002)

Test the registry endpoints.

| Component             | Command                               | Expected Response |
| :-------------------- | :------------------------------------ | :---------------- |
| **Proxy (Pushing)** |  `curl http://localhost:15000/v2/`     | `{}`              |
| **Kraken Agent One (Pulling)** |  `curl http://localhost:16000/v2/`     | `{}`              |
| **Kraken Agent Two (Pulling)** |  `curl http://localhost:17000/v2/`     | `{}`              |
| **Backend Storage** | `curl http://localhost:14000/health`  | `OK`              |

All should return successful responses with **HTTP 200 OK** status.

## Step 4: Test Image Push and Pull

### Push an Image to Kraken

```bash
# Pull an example image
docker pull hello-world

# Tag the image for Kraken
docker tag hello-world localhost:15000/test/hello-world:latest

# Push to Kraken (goes through the proxy)
docker push localhost:15000/test/hello-world:latest
```

### Pull Images from Agents (P2P Distribution)

```bash
# Pull from agent one
docker pull localhost:16000/test/hello-world:latest

# Pull from agent two (this should use P2P to get data from agent one)
docker pull localhost:17000/test/hello-world:latest
```

### Test with Different Images

```bash
# Try with a larger image to see more P2P benefits
docker pull nginx:latest

# Tag and push to Kraken
docker tag nginx:latest localhost:15000/test/nginx:latest
docker push localhost:15000/test/nginx:latest

# Pull from both agents
docker pull localhost:16000/test/nginx:latest
docker pull localhost:17000/test/nginx:latest
```

## Step 5: Monitor P2P Activity

To see the P2P distribution in action, monitor the logs:

```bash
# Watch herd logs (shows proxy, origin, tracker activity)
docker logs -f kraken-herd

# In separate terminals, watch agent logs
docker logs -f kraken-agent-one
docker logs -f kraken-agent-two
```

Look for log entries showing:
- Peer announcements to the tracker
- Blob transfers between agents
- Origin seeding activity

## Step 6: Understanding the Architecture

### Port Mapping

| Component | Port | Purpose |
|-----------|------|---------|
| **Proxy** | 15000 | Push images here |
| **Agent One** | 16000 | Pull images (P2P enabled) |
| **Agent Two** | 17000 | Pull images (P2P enabled) |
| **TestFS Backend** | 14000 | File storage backend |
| **Origin Server** | 15002 | Internal blob server |
| **Tracker** | 15003 | Internal peer coordination |
| **Build-Index** | 15004 | Internal tag mapping |

### How It Works

1. **Push Flow**: `docker push localhost:15000/...` → Proxy → Origin → TestFS storage
2. **Pull Flow**: `docker pull localhost:16000/...` → Agent → Tracker (find peers) → P2P download from other agents/origin
3. **P2P Magic**: When multiple agents pull the same image, they share data directly with each other

## Step 7: Advanced Testing

### Test P2P Benefits

To really see the P2P benefits, try pushing and pulling larger images:

```bash
# Use a larger image
docker pull postgres:latest

# Tag and push to Kraken
docker tag postgres:latest localhost:15000/test/postgres:latest
docker push localhost:15000/test/postgres:latest

# Clear local images to force download
docker rmi postgres:latest localhost:15000/test/postgres:latest

# Pull from multiple agents simultaneously
docker pull localhost:16000/test/postgres:latest &
docker pull localhost:17000/test/postgres:latest &
wait
```

Monitor the logs to see how the agents coordinate and share data.

### Cleanup

To stop and remove all Kraken containers:

```bash
make docker_stop
```

## Troubleshooting

### Common Issues

#### **403 Forbidden on Agent Endpoints (Port Blocking)**

**Problem**: Getting 403 Forbidden when testing agent endpoints.
```bash
curl http://localhost:16000/v2/
# Returns: 403 Forbidden nginx/1.18.0

curl http://localhost:17000/v2/
# Returns: 403 Forbidden nginx/1.18.0
```

**Root Cause**: Firewalls or softwares often block non-standard ports like 16000-17000.

**Solution 1: Use Standard Ports (Recommended)**

Edit `examples/devcluster/agent_one_param.sh`:
```bash
# Define agent ports.
AGENT_REGISTRY_PORT=8080  # Changed from 16000
AGENT_PEER_PORT=8081      # Changed from 16001
AGENT_SERVER_PORT=8082    # Changed from 16002

# Rest of file unchanged...
```

Edit `examples/devcluster/agent_two_param.sh`:
```bash
# Define agent ports.
AGENT_REGISTRY_PORT=8090  # Changed from 17000
AGENT_PEER_PORT=8091      # Changed from 17001
AGENT_SERVER_PORT=8092    # Changed from 17002

# Rest of file unchanged...
```

Restart the cluster.
```bash
make docker_stop
make devcluster
```

Test with new ports.
```bash
curl http://localhost:8080/v2/   # Should return: {}
curl http://localhost:8090/v2/   # Should return: {}
```

**Solution 2: Diagnostic Commands**

To confirm if it's a firewall issue:
```bash
# Test from inside containers (bypasses firewall)
docker exec kraken-agent-one curl -s http://localhost:16000/v2/
docker exec kraken-agent-two curl -s http://localhost:17000/v2/

# Check port connectivity
nc -zv localhost 16000
nc -zv localhost 17000

# Verify Docker port mapping
docker port kraken-agent-one
docker port kraken-agent-two
```

If internal container tests return `{}` but external tests fail, it confirms firewall blocking.

#### **Other Common Issues**

- **Port Conflicts**: Make sure ports are not in use by other applications
- **Docker Desktop**: Make sure Docker Desktop is running and `host.docker.internal` is available

### Getting Help

- Check container logs: `docker logs <container-name>`
- Verify container status: `docker ps -a`
- Check port availability: `netstat -an | grep <port>`

## What You've Accomplished

**Congratulations!** You've successfully

1. Set up a complete Kraken P2P Docker registry
2. Tested image push and pull workflows
3. Experienced P2P distribution in action
4. Learned how Kraken's architecture works

## Next Steps

- Explore Kraken's production configuration options
- Integrate with cloud storage backends (S3, GCS, etc.)
- Set up monitoring and metrics
- Deploy in a Kubernetes environment
- Read the [official documentation](docs/CONFIGURATION.md) for advanced features

Happy P2P container distribution! 🐙
