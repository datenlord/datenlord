# Operating requirement

- Run the scripts with bash, because we need to load env variables from cron.yml

- Run the third script in common user, the others in root

- Run 4.1 after `cargo build --release`

- If there's problem after 4.2, just check your proxy setting and switch proxy node.

- Make sure the virtual machine has enough resouces, 8g mem, 4 cores, 200g disk.

# Practical steps

1. Create a vm for test. Install some basic software: ssh, vim, cmake, protobuf, rust, curl, take snapshot for the vm as `Snapshot1`
2. Run scripts 1-3
   
   (script1, root) `bash scripts/local_test/1_install_kind.sh`
   
   ---
   
   (script2, root) `bash scripts/local_test/2_kind_cluster.sh`

   When running 2, you should make sure the network is ok, if failed the config the proxy like following guidance. If after all these done and you still failing, just roll back to `SnapShot1` 

   --- 

   (script3, common user) `bash scripts/local_test/3_set_ssh.sh`
  
   

   When running 3, you should make sure you run it in common user and give the user docker permission as following.

   Run `cargo build --release`

   After all these were done, take `Snapshot2`
3. Each time the code changed, just rollback to `Snapshot2` , run `cargo build --release` and run 4.1 to make the docker image
   
   (script4.1, root) `bash scripts/local_test/4_redeploy_after_build/1_image.sh`
4. After the docker image is prepared, run 4.2. If 4.2 failed, check the network or switch proxy node then retry 4.2
   
   (script4.2, root) `bash scripts/local_test/2_load.sh`

5. Run 5 and 6, the log will be written in project dir.
   
   (script5, root) `bash scripts/local_test/5_csi_e2e_test.sh`

   (script6, root) `bash scripts/local_test/6_get_logs.sh`
# Permission related

- docker for common user
  ``` bash
  sudo gpasswd -a $USER docker  #将登陆用户加入到docker用户组中
  newgrp docker #更新用户组
  ```

# Proxy may need

- curl
  ```
  #Example proxy_server_url: http://192.168.232.1:23333
  export http_proxy="proxy_server_url" 
  export https_proxy="proxy_server_url"
  ```
  
- docker

  `vim ~/.docker/config.json`

  ```
  #Example proxy_server_url: http://192.168.232.1:23333
  {
    "proxies": {
      "default": {
        "httpProxy": "proxy_server_url",
        "httpsProxy": "proxy_server_url",
        "noProxy": "169.254.169.254,kind-control-plane,*.test.example.com,.example.org,127.0.0.0/8"
      }
    }
  }
  ```

  ```
  vim /usr/lib/systemd/system/docker.service

  Add proxy under [Service]
  #Example proxy_server_url: http://192.168.232.1:23333

  Environment="HTTP_PROXY=proxy_server_url"
  Environment="HTTPS_PROXY=proxy_server_url"

  systemctl daemon-reload
  systemctl restart docker.service
  ```