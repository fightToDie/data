#!/bin/sh

#Category arguments
#args=("hiphop" "rock" "jazz" "pop" "classic" "blues" "country" "disco")
args=("hiphop" "rock" "jazz")

# Iterate over the arguments and run the Python script with each argument
for arg in "${args[@]}"; do
  python D:/pythonProject/main.py "$arg" &
done

wait

read -p "Press any key to exit..."
