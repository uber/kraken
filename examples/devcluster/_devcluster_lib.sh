# Shared helpers sourced by the devcluster start scripts. Sets up a
# user-defined bridge network so containers can reach each other by name
# (`host.docker.internal` is set as a network alias on the herd) without
# depending on Docker Desktop's host.docker.internal magic. The harness
# on the host still talks to containers via their published ports.

KRAKEN_DEVCLUSTER_NETWORK=${KRAKEN_DEVCLUSTER_NETWORK:-kraken-bench}

# Create the network if it does not exist. Silent if already present.
if ! docker network inspect "${KRAKEN_DEVCLUSTER_NETWORK}" >/dev/null 2>&1; then
    docker network create "${KRAKEN_DEVCLUSTER_NETWORK}" >/dev/null
fi

# Args every container gets. The herd additionally adds --network-alias
# host.docker.internal so the existing configs that point at
# host.docker.internal:<port> still work for inter-container traffic.
NETWORK_ARGS="--network ${KRAKEN_DEVCLUSTER_NETWORK}"
HERD_NETWORK_ARGS="${NETWORK_ARGS} --network-alias host.docker.internal"

