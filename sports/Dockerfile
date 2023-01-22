FROM golang:alpine

# Install dependencies
RUN apk add --no-cache build-base curl git make protobuf-dev unzip

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
    protoc --go_out=. --go-grpc_out=require_unimplemented_servers=false:. sports/sports.proto

# Build
RUN go build

ARG SPORTS_ENDPOINT

CMD ["sh", "-c", "./sports -grpc-endpoint $SPORTS_ENDPOINT"]



