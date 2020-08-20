# manager

* metadata-manager
    * Manages metadata 
* message-broker
    * Mediates communication among components in order to be able to exchange messages

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake doxygen libboost-system-dev
```

## How to build

```sh
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```

available options:
* `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen

### install 

```sh
make install
```

### generate documents

```sh
make doxygen
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
