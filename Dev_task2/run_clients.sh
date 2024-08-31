!/bin/bash

# Start the server
node server.js &

# Start multiple clients
NUM_CLIENTS=3  # Adjust this number as needed

for ((i = 1; i <= NUM_CLIENTS; i++))
do
    node client.js &
done

wait  # Wait for all background processes to finish
