# metadata-manager
* Manages metadata 

## Requirements

* CMake `>= 3.10`
* C++ Compiler `>= C++17`

## How to build

```sh
apt update -y && apt install -y git build-essential cmake doxygen
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
