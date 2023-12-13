# CSI Test Overview
The `csi-test sanity` ensures that a CSI driver conforms to the CSI specification by calling the gRPC methods in various ways and checking that the outcome is as required. Despite its current hosting under the Kubernetes-CSI organization, it is completely independent of Kubernetes. Tests connect to a running CSI driver through its Unix domain socket, so although the tests are written in Go, the driver itself can be implemented in any language.

# Building csi-test
To build csi-test, follow these steps:

1. Clone the `csi-test` repository:
```shell
git clone https://github.com/kubernetes-csi/csi-test
cd csi-test/cmd/csi-sanity
```
2. Build the project using Go:
```shell
go build
```

# Starting the test
The testing process requires running two separate services: the Node Service and the Controller Service. These services are responsible for testing the respective components of the CSI driver

1. Start Node Service(from the Datenlord project directory):
```shell
./scripts/setup/start_local_node.sh
```

2. After ensuring that the Node Service has started successfully, start the Controller Service (from the Datenlord project directory)
```shell
./scripts/setup/start_local_controller.sh
```

3. Finally, execute the csi-test (from the csi-test project directory):
```shell
export NODE_SOCKET_FILE=/tmp/node.sock
export CONTROLLER_SOCKET_FILE=/tmp/controller.sock
.//cmd/csi-sanity/csi-sanity -csi.endpoint=$NODE_SOCKET_FILE -csi.controllerendpoint=$CONTROLLER_SOCKET_FILE --ginkgo.flake-attempts 3
```

# Interpreting Test Results
A successful test run will display an output similar to:
```log
Ran 57 of 84 Specs in 5.490 seconds
SUCCESS! -- 57 Passed | 0 Failed | 1 Pending | 26 Skipped
```
In case of a failure, you might encounter an error like:
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
This could indicate a misconfiguration in the `bind_mounter`. Consult the bind_mounter's log (typically located at `/datenlord_repo/datenlord_bind_mounter.log`) for detailed error information. If you identify an error, consider reporting it to the Datenlord project

For other types of errors, it's important to review any recent changes to the system or code, as they may be causing the test failures