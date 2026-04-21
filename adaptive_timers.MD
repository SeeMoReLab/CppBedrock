# Bedrock SBFT for Adaptive Timers

- Setup

    - Edit `config/config.entities.yaml` to change server count
    - Edit `config/config.sbft.yaml` for epoch size, agent ports and default timers

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

- Run Bedrock SBFT

    Agents disabled `./CppBedrock`

    Agents enabled `./CppBedrock --agent`

- Run clients in another terminal

    `cd build`

    `./integration_test 4 [total_tx_count] [num_clients]

# Fault schedule

Edit `config/config.sbft.byzantine.yaml`
