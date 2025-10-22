#!/bin/bash
set -e

echo "=== Chorus Setup Script ==="
echo ""

# Check if Go is installed
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed. Please install Go first."
    exit 1
fi

echo "✓ Go is installed"

# Install protoc-gen-go and protoc-gen-go-grpc
echo ""
echo "Installing protoc Go plugins..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

echo "✓ Protoc plugins installed"

if ! command -v protoc &> /dev/null; then
    echo ""
    echo "Warning: protoc is not installed."
    echo "Please install it:"
    echo "  - macOS: brew install protobuf"
    echo "  - Linux: apt-get install protobuf-compiler"
    echo "  - Or download from: https://github.com/protocolbuffers/protobuf/releases"
    exit 1
fi

echo "✓ protoc is installed"

echo ""
echo "Generating proto files..."
cd node
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/node.proto

echo "✓ Proto files generated"

# Download Go dependencies
echo ""
echo "Downloading Go dependencies..."
go mod tidy

echo "✓ Dependencies downloaded"

echo ""
echo "=== Setup Complete ==="
echo ""
echo "You can now run:"
echo "  cd node"
echo "  go run main.go"