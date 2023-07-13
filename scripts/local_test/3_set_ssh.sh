source scripts/local_test/load_envs.sh


sudo rm -f /tmp/sources.list
sudo rm -f /tmp/setup.sh

sudo cat > /tmp/sources.list <<'END'
# ubuntu20
deb http://mirrors.aliyun.com/ubuntu/ focal main restricted
deb http://mirrors.aliyun.com/ubuntu/ focal-updates main restricted
deb http://mirrors.aliyun.com/ubuntu/ focal universe
deb http://mirrors.aliyun.com/ubuntu/ focal-updates universe
deb http://mirrors.aliyun.com/ubuntu/ focal multiverse
deb http://mirrors.aliyun.com/ubuntu/ focal-updates multiverse
deb http://mirrors.aliyun.com/ubuntu/ focal-backports main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ focal-security main restricted
deb http://mirrors.aliyun.com/ubuntu/ focal-security universe
deb http://mirrors.aliyun.com/ubuntu/ focal-security multiverse
END

sudo cat >>/tmp/setup.sh <<'END'
NODES_IP="$(sudo kubectl get nodes -A -o wide | awk 'FNR > 2 {print $6}')"
NODES="$(sudo kubectl get nodes -A -o wide | awk 'FNR > 2 {print $1}')"
for node in ${NODES}; do
    USER="$(whoami)"
    #docker cp /etc/apt/sources.list ${node}:/etc/apt/sources.list
    docker cp /tmp/sources.list ${node}:/etc/apt/sources.list
    docker exec ${node} apt-get update
    docker exec ${node} apt-get install -y ssh sudo
    docker exec ${node} systemctl start sshd
    docker exec ${node} useradd -m ${USER}
    docker exec ${node} usermod -aG sudo ${USER}
    echo "${USER} ALL=(ALL) NOPASSWD:ALL" > /tmp/${USER}
    docker cp /tmp/${USER} ${node}:/etc/sudoers.d/${USER}
    docker exec ${node} chown root:root /etc/sudoers.d/${USER}

    docker exec ${node} mkdir /home/${USER}/.ssh
    docker exec ${node} ls -al /home/${USER}/
    
    docker cp ${HOME}/.ssh/id_rsa.pub ${node}:/home/${USER}/.ssh/authorized_keys
    docker exec ${node} chown ${USER}:${USER} /home/${USER}/ -R
done
for ip in ${NODES_IP}; do
    ssh-keyscan -H $ip >> ${HOME}/.ssh/known_hosts
done
END

rm -rf $HOME/.ssh/
ssh-keygen -N '' -f ~/.ssh/id_rsa
/bin/bash /tmp/setup.sh