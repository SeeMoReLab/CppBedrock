# Bedrock SBFT for Adaptive Timers

- Clear old build

    `rm ./build`

    `mkdir build`
    
    `cd build`
- Build

    `cmake ..`

    `make`

- Run Bedrock SBFT

    `./CppBedrock`

- Run clients in another terminal

    `cd build`

    `./integration_test 4 [total_tx_count] [num_clients]

# Fault schedule

Edit `config/config.sbft.byzantine.yaml`
