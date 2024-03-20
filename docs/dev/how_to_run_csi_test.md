# CSI Test Overview
The `csi-test sanity` validates the adherence of a CSI driver to the CSI specification by invoking various gRPC methods and verifying that the results align with the expected outcomes. Although it is currently hosted under the Kubernetes-CSI organization, it maintains complete independence from Kubernetes. The tests establish a connection with an active CSI driver through its Unix domain socket. Therefore, while the tests are implemented in Go, there is flexibility in the implementation language of the driver, allowing it to be written in any programming language.

# Building csi-test
To build csi-test, please follow these steps:

1. Clone the `csi-test` repository:
```shell
git clone https://github.com/kubernetes-csi/csi-test
cd csi-test/cmd/csi-sanity
```
2. Build the project using Go:
```shell
go build
```

# Run Tests
The testing process requires the execution of two distinct services: CSI Node and CSI Controller. These services are responsible for testing the respective components of the CSI driver.

1. Start CSI Node (from the Datenlord project directory):
```shell
./scripts/setup/start_local_node.sh
```

2. After the CSI Node has started successfully, start CSI Controller (from the Datenlord project directory)
```shell
./scripts/setup/start_local_controller.sh
```

3. Finally, execute the csi-test (from the csi-test project directory):
```shell
export NODE_SOCKET_FILE=/tmp/node.sock
export CONTROLLER_SOCKET_FILE=/tmp/controller.sock
./cmd/csi-sanity/csi-sanity -csi.endpoint=$NODE_SOCKET_FILE -csi.controllerendpoint=$CONTROLLER_SOCKET_FILE --ginkgo.flake-attempts 3
```

# Interpreting Test Results
A successful test running will print an output similar to:
```log
Ran 57 of 84 Specs in 5.490 seconds
SUCCESS! -- 57 Passed | 0 Failed | 1 Pending | 26 Skipped
```
In case of a failure, you may see an error message like:
```log
 [FAILED] failed to create target directory 
  Unexpected error:
      <*fs.PathError | 0xc00042b4a0>: 
      mkdir /tmp/csi-mount: file exists
      {
          Op: "mkdir",
          Path: "/tmp/csi-mount",
          Err: <syscall.Errno>0x11,
      }
```
This could be caused by a misconfiguration in the `bind_mounter`. Consult the log of bind_mounter (typically located at `/datenlord_repo/datenlord_bind_mounter.log`) for details. If you identify an error, please take an consideration to report it to us via an issue.

For other types of errors, it's important to review any recent changes to the system or code, as they may cause failures in the test.