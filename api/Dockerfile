FROM golang:alpine

# Install dependencies
RUN apk add --no-cache curl git make protobuf-dev unzip

WORKDIR /src

COPY . .

# Install Protoc from binaries to obtain the /include folder
RUN PROTOC_ZIP=protoc-21.12-linux-x86_64.zip && \
    curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v21.12/$PROTOC_ZIP && \
    unzip -o $PROTOC_ZIP -d /usr/local bin/protoc && \
    unzip -o $PROTOC_ZIP -d /usr/local 'include/*' && \
    rm -f $PROTOC_ZIP

# Fetch all the dependencies
RUN go get .

# Install Protoc-Gen-Go
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2 && \
    go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway && \
    go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2 && \
    go install google.golang.org/genproto/googleapis/api && \
    export PATH="$PATH:$(go env GOPATH)/bin"

# Generate Proto files
RUN cd ./proto && \
    protoc -I /usr/local/include -I . --go_out . --go_opt paths=source_relative --go-grpc_out . --go-grpc_opt paths=source_relative --grpc-gateway_out . --grpc-gateway_opt paths=source_relative racing/racing.proto sports/sports.proto

# Build
RUN go build

# Get the endpoint locations
ARG API_ENDPOINT
ARG RACING_ENDPOINT
ARG SPORTS_ENDPOINT

CMD ["sh", "-c", "./api -api-endpoint $API_ENDPOINT -racing-endpoint $RACING_ENDPOINT -sports-endpoint $SPORTS_ENDPOINT"]