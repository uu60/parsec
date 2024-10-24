# MPC Secret Sharing Package
## 1 Introduction
A C++ package for implementing MPC secret sharing operations (including Oblivious Transfer, etc.) conveniently based on OpenMPI framework. This package is built for 3-party situation, including one client and two servers.

The communicating framework can be changed according to real situation. Changing `utils/Mpi` class can change that.

> There is a test case project of the mpc-package, which is https://github.com/uu60/mpc-package-test.

Currently, the secret can be bool, int8, int16, int32 and int64. All the classes are implemented as template classes. So when you use them, please clarify the secret's type (like `AdditionExecutor<int64_t>`).
## 2 Usage
### 2.1 OpenMPI Environment
#### 2.1.1 For macOS
- execute
`brew install openmpi`
#### 2.1.2 For Linux (Ubuntu)
- use source code to compile and install.
1. `wget https://download.open-mpi.org/release/open-mpi/v4.1/openmpi-4.1.5.tar.gz` (use same version for devices)
2. `tar -xzvf openmpi-_data._data._data.tar.gz`
3. `cd openmpi-_data._data._data`
4. `./configure --prefix=/usr/local/openmpi`
5. `make`
6. `sudo make install`
7. Add the following content into environment variable configuration file like `.bashrc`
    ```shell
    export PATH=/usr/local/openmpi/bin:$PATH
    export LD_LIBRARY_PATH=/usr/local/openmpi/lib:$LD_LIBRARY_PATH
    ```
8. `source ~/.bashrc`

### 2.2 Install this package
1. Download the whole project
2. `cd <project root>`
3. `sh install.sh`
> ⚠️ `install.sh` will delete former mpc_package.
### 2.3 Import this package in your project
CMakeLists.txt should contain the following content:
```txt
find_package(mpc_package REQUIRED)

target_link_directories(demo PUBLIC ${mpc_package_LIBRARY_DIRS})
target_link_libraries(demo mpc_package)
target_include_directories(demo PUBLIC ${mpc_package_INCLUDE_DIRS})
```
which will imports the package into your project.
### 2.4 Execute
1. Use the same username of Linux/macOS.
2. Set up non-password login between 2 machines. Using `ssh-keygen` and `ssh-copy-id`.
3. Place the executable file on the same path of 2 machines.
4. Edit a **hostfile.txt**, which records IP addresses of the 2 machines. (Or use host name by editing **/etc/hosts**) Write slot number for each machine.
   <br>(The format of host file can be referred in **hostfile.txt** in this project)
5. To distinguish executing machine, edit their hostnames by executing `sudo hostnamectl set-hostname ub1`. **ub1** is the new host name.
6. Your project should be complied by **CMake** instead of `mpicxx`. 
7. Then use `mpirun` to execute it. Here is an example shell:
    ```shell
    mpirun -np 2 -hostfile hostfile.txt a.out
    ```
## 3 Current Functions
- Arithmetic Share:
   - Multiplication Share with **RSA OT BMT** or **pre-generated BMT**
   - Addition Share 
   - Comparison
   - Conversion between boolean and arithmetic
   - Multiplex
- Boolean Share:
  - And Share with **RSA OT BMT** or **pre-generated BMT**
  - XOR Share
- Utility functions (under `utils` directory)
> There are some new classes under api/ directory which can execute MPC more easily. Please refer to test cases for their usage.
## 4 Test cases
> There is a test case project of the mpc-package, which is https://github.com/uu60/mpc-package-test.
Test case project will be compiled as `demo`. Please execute by:
```shell
cd <demo-project>
cmake .
make
mpirun -np 2 -hostfile hostfile.txt demo
```
There is a script file `test.sh` under shell_scripts/ directory which can conveniently upload and compile the project to your test server. You can modify details for your situation.
## Main References
[1] ABY (https://encrypto.de/papers/DSZ15.pdf) \
[2] Crypten (https://arxiv.org/pdf/2109.00984)
  


