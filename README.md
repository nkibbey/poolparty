# Pool Party

## Why the name?
Named pool party because of pool and cues, this is a queue app and I like to keep project names interesting.

## Description
Personal project to show off basic queue impl

## Depends
- Go
- (optional) docker or podman for image building
- (optional) gnu make

## Build-it
```
make build
# or alternatively yourself (less preferred)
mkdir -p bin
go build -o bin ./...
```

## Run-it
```
./bin/poolparty
# check out the build info
./bin/poolparty -v
```

## Ship-it
```
make all
docker push docker.io/nkibbey/poolparty:latest
```
