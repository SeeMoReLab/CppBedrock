# CppBedrock for Adaptive Timers

- Setup

    - Edit `config/config.entities.yaml` to change server count
    - Edit `config/runtime.selection.yaml` to select the protocol (PBFT, LinearPBFT, SBFT, Hotstuff, Hotstuff2, ChainedHotstuff, Zyzzyva)
    - Edit the selected protocol's `config/config.<protocol>.yaml` for default timers and the learning-agent settings

- Generate server keys

    `cd keys`

    `./gen_server_keys.sh {num_servers}`

- Generate client keys

    From project root

    `openssl genpkey -algorithm RSA -out keys/client_private.pem -pkeyopt rsa_keygen_bits:2048`

    `openssl rsa -pubout -in keys/client_private.pem -out keys/client_public.pem`

- Clear old build

    `rm ./build`

    `mkdir build`
    
    `cd build`
- Build

    `cmake ..`

    `make`

- Run Bedrock

    Agents disabled `./CppBedrock`

    Agents enabled `./CppBedrock --agent` (or set `agent.enabled: true` in the protocol config)

- Run clients in another terminal

    `cd build`

    `./integration_test 4 [total_tx_count] [num_clients]

# Learning agent (wall-clock mode)

Learning-agent communication is protocol-agnostic and works with every protocol config. The per-protocol report and timeout messages live in `proto/agent.proto`; the wall-clock cycle and the protocol adapters live in `src/core/agent/AgentClient.cpp`.

Each node connects to a local LearningAgent gRPC service (`agent.ports` maps node id to port) and repeats this wall-clock episode cycle:

1. Feature collection for `agent.feature_duration_ms`: consensus latency, phase latencies, throughput, and view changes are accumulated, then sent to the agent as a report (`window_consensus_count = 0` marks the wall-clock window). The previous episode's reward is attached to this report.
2. Reply wait for `agent.reply_wait_ms`: the node polls `GetTimeout` for the recommendation and applies it at the deadline (view-change/election timeout for every protocol, plus the slow-path timeout for SBFT).
3. Warm-up for `agent.warmup_duration_ms`: metrics are discarded while the system settles under the new timeouts.
4. Reward collection for `agent.reward_duration_ms`: metrics under the new timeouts become the reward, delivered with the next episode's report.

Protocol mapping: PBFT and LinearPBFT report as `PROTOCOL_PBFT`, SBFT as `PROTOCOL_SBFT`, the HotStuff variants as `PROTOCOL_HOTSTUFF`, and Zyzzyva as `PROTOCOL_ZYZZYVA`. Start the matching agent, for example `python3 learning_agent/main.py --node-id 1 --protocol pbft ...`.

A live smoke test is available: with an agent running on port 50190, run `./build/agent_client_smoke PBFT 50190 12`.

# Fault schedule

Edit `config/config.sbft.byzantine.yaml`
