.PHONY: build run-node1 run-node2 run-node3 run-node4 run-node5 clean

# Build the binary
build:
	go build -o chorus-node .

# Run individual nodes
run-node1: build
	NODE_ID=node1 PORT=8010 ./chorus-node

run-node2: build
	NODE_ID=node2 PORT=8011 ./chorus-node

run-node3: build
	NODE_ID=node3 PORT=8012 ./chorus-node

run-node4: build
	NODE_ID=node4 PORT=8013 ./chorus-node

run-node5: build
	NODE_ID=node5 PORT=8014 ./chorus-node

# Clean up
clean:
	rm -f chorus-node
