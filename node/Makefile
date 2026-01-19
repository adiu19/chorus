.PHONY: build run-node1 run-node2 run-node3 run-all clean

# Build the binary
build:
	go build -o chorus-node .

# Run individual nodes
run-node1: build
	NODE_ID=node1 PORT=8001 ./chorus-node

run-node2: build
	NODE_ID=node2 PORT=8002 ./chorus-node

run-node3: build
	NODE_ID=node3 PORT=8003 ./chorus-node

# Clean up
clean:
	rm -f chorus-node
