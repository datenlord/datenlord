#FOUND_PATH=`curl --silent prometheus-service.datenlord-monitoring.svc:8080 | grep Found | awk '{print $0}'`
#echo ${#FOUND_PATH}
#if test -n #FOUND_PATH
#then
#  echo 'aa'
#else
#  echo 'bb'
#fi

RESULT=`promtool test rules alertmanager_test.yaml | grep SUCCESS | awk '{print $0}'`
echo ${#RESULT}
if test ${#RESULT} -eq 0
then 
  echo 'aa'
else
  echo 'bb'
fi


#NODES_IP="$(kubectl get nodes -A -o wide | awk 'FNR == 2 {print $6}')"
#echo ${NODES_IP}

#for ip in ${NODES_IP}; do
#  echo ${ip}
#done

#POD_NAME=kubectl get pods -l app=prometheus-server -n datenlord-monitoring | grep prometheus | awk '{print $1}' | head -n 1
#echo $POD_NAME
#kubectl port-forward $POD_NAME 8080:9090 -n datenlord-monitoring

#aa="${NODES_IP} | awk '{print $0;}'"
#echo ${NODES_IP[0]}
#echo ${aa}
