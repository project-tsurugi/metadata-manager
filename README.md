# metadata manager

## Requirements

- CMake `>= 3.10`
- C++ Compiler `>= C++17`
- libpq

### Dockerfile

```dockerfile
FROM ubuntu:22.04

RUN apt update -y && apt install -y git build-essential cmake ninja-build doxygen libboost-system-dev libboost-filesystem-dev
```

## How to initially set up the metadata manager

### for PostgreSQL

When PostgreSQL is specified as the data storage for metadata.
See [How to build](#how-to-build) for how to specify.

1. Start PostgreSQL server.

2. Define metadata tables and load initial metadata.  

   ```sh
   psql postgres < sql/ddl.sql
   ```  

   The default database name is `tsurugi`.  
   How to change the database name.  

   1. Change the database name in the `sql/ddl.sql` file.  
      `CREATE DATABASE your-database-name LC_COLLATE 'C' LC_CTYPE 'C' ENCODING 'UTF8' template=template0;`  
      `\c your-database-name`  

   2. Set the database name to the environment variable (`TSURUGI_CONNECTION_STRING`).  
      `export TSURUGI_CONNECTION_STRING="dbname=your-database-name"`

### for JSON

When file (JSON) is specified as the data storage for metadata.
See [How to build](#how-to-build) for how to specify.

1. Create a directory to store metadata.

   ```sh
   mkdir -p $HOME/.local/tsurugi/metadata
   ```

   You can change the above path by set the environment variable (`TSURUGI_METADATA_DIR`).
   The directory must exist.

   ```sh
   export TSURUGI_METADATA_DIR="your-directory-name"
   ```

## How to build

1. (for PostgreSQL) Make sure that `pg_config` is in the PATH (test with `pg_config --pgxs`).

2. Retrieve third party modules.

   ```sh
   git submodule update --init --recursive
   ```

3. Build manager.

   ```sh
   mkdir build
   cd build
   cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug ..
   ninja
   ```

   available options:
     - `-DCMAKE_INSTALL_PREFIX=<installation directory>` - change install location
     - `-DBUILD_DOCUMENTS=OFF` - never build documents by doxygen
     - `-DDATA_STORAGE=postgresql` - specifies the data storage where the metadata is stored: `postgresql`, `json`
     - `-DLOG_LEVEL=error` - specifies the default log output level (minimum) for metadata-manager: `none`, `err[or]`, `warn[ing]`, `info`, `debug`
     - for debugging only
       - `-DENABLE_SANITIZER=OFF` - disable sanitizers (requires `-DCMAKE_BUILD_TYPE=Debug`)
       - `-DENABLE_UB_SANITIZER=ON` - enable undefined behavior sanitizer (requires `-DENABLE_SANITIZER=ON`)
       - `-DENABLE_COVERAGE=ON` - enable code coverage analysis (requires `-DCMAKE_BUILD_TYPE=Debug`)

### install

```sh
ninja install
```

### run tests

1. (for PostgreSQL) Update the shared library search path for libpq.  
   The method to set the shared library search path varies between platforms,  
   but the most widely-used method is to set the environment variable LD_LIBRARY_PATH like so:  
   In Bourne shells (sh, ksh, bash, zsh):  

   ```sh
   export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:<PostgreSQL install directory>/lib
   ```

2. run tests

   ```sh
   cd build
   ctest -V
   ```

### generate documents

```sh
mkdir build
cd build
cmake -G Ninja -DBUILD_DOCUMENTS=ON ..
ninja doxygen
```

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
