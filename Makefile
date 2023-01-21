# Useful shortcuts to streamline development

build: build-api build-racing build-sports

build-api:
	cd ./api && \
	go build

build-racing:
	cd ./racing && \
	go build

build-sports:
	cd ./sports && \
	go build

generate: generate-api generate-racing generate-sports

generate-api:
	cd ./api && \
	go generate ./...

generate-racing:
	cd ./racing && \
	go generate ./...

generate-sports:
	cd ./sports && \
	go generate ./...

install-dependencies:
	cd ./api && \
	go get . && \
	cd ../racing && \
	go get . && \
	cd ../sports && \
	go get .

install-protoc-gen-go:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28 && \
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2

lint: lint-api lint-racing lint-sports

lint-api:
	cd ./api && \
	golangci-lint run ./...

lint-racing:
	cd ./racing && \
	golangci-lint run ./...

lint-sports:
	cd ./sports && \
	golangci-lint run ./...

test: test-api test-racing test-sports

test-api:
	cd ./api && \
	go test ./...

test-racing:
	cd ./racing && \
	go test ./...

test-sports:
	cd ./sports && \
	go test ./...