# Useful shortcuts to streamline development

build: build-api build-racing

build-api:
	cd ./api && \
	go build

build-racing:
	cd ./racing && \
	go build

generate: generate-api generate-racing

generate-api:
	cd ./api && \
	go generate ./...

generate-racing:
	cd ./racing && \
	go generate ./...

install-dependencies:
	cd ./api && \
	go get . && \
	cd ../racing && \
	go get .

lint: lint-api lint-racing

lint-api:
	cd ./api && \
	golangci-lint run ./...

lint-racing:
	cd ./racing && \
	golangci-lint run ./...

test: test-api test-racing

test-api:
	cd ./api && \
	go test ./...

test-racing:
	cd ./racing && \
	go test ./...