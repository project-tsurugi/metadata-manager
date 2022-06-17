# manager

- metadata-manager
  - Manages metadata
- message-broker
  - Mediates communication among components in order to be able to exchange messages
- authentication-manager
  - Provides user authentication using PostgreSQL.

## Requirements

- CMake `>= 3.10`
- C++ Compiler `>= C++17`
- libpq

### Dockerfile

```dockerfile
FROM ubuntu:18.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build doxygen libboost-system-dev libboost-filesystem-dev
```

## How to build

1. Make sure that `pg_config` is in the PATH (test with `pg_config --pgxs`).

1. Start PostgreSQL server.

1. Define metadata tables and load initial metadata.

    ```sh
    psql postgres < sql/ddl.sql
    ```

    The default database name is `tsurugi`.  
    How to change the database name.

     1. Change the database name in the `sql/ddl.sql` file.
        > CREATE DATABASE *`database-name`* LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8' template=template0;
        >  
        > \c *`database-name`*

     1. Set the database name to the environment variable (TSURUGI_CONNECTION_STRING).
        > export TSURUGI_CONNECTION_STRING=dbname=*`database-name`*

1. Retrieve third party modules.

    ```sh
    git submodule update --init --recursive
    ```

1. Build manager.

    ```sh
    mkdir build
    cd build
    cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
    ninja
    ```

available options:

- `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
- `-DDATA_STORAGE=postgresql` - specifies the data-storage where the metadata is stored. "postgresql" or "json".
- `-DBUILD_TARGET=ALL` - specifies the library to be built. "ALL" or "AUTH" or "METADATA".
- for debugging only
  - `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
  - `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
  - `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)

### install

```sh
ninja install
```

- Notice

  If you chose `-DDATA_STORAGE=json` option, you should make the following directory manually.

  `$HOME/.local/tsurugi/metadata`

  And you can change the above path by set the following environment variable.

  `TSURUGI_METADATA_DIR`

### run tests

1. Update the shared library search path for libpq.  
  The method to set the shared library search path varies between platforms,  
  but the most widely-used method is to set the environment variable LD_LIBRARY_PATH like so:  
  In Bourne shells (sh, ksh, bash, zsh):  

    ```sh
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<PostgreSQL install directory>/lib
    ```

1. run tests

    ```sh
    ctest -V
    ```

### generate documents

```sh
make doxygen
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
