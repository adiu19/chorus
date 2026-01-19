# Build stage
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache protobuf protobuf-dev git

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.35.1
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

COPY proto/ ./proto/
RUN protoc --go_out=. --go_opt=paths=source_relative \
           --go-grpc_out=. --go-grpc_opt=paths=source_relative \
           proto/node.proto

COPY *.go ./
COPY config/ ./config/
COPY cluster/ ./cluster/
COPY ring/ ./ring/

RUN CGO_ENABLED=0 GOOS=linux go build -o chorus-node .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /build/chorus-node .
COPY config.yaml .

EXPOSE 8001

CMD ["./chorus-node"]