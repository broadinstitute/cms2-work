#!/bin/bash
# A wrapper around /entrypoint.sh to trap the SIGINT signal (Ctrl+C) and forwards it to the mysql daemon
# In other words : traps SIGINT and SIGTERM signals and forwards them to the child process as SIGTERM signals

#set +x -e -o pipefail

set +x

#trap cleanupFunction 0

#myNormalDaemonProcess &

#wait

/wait-for-it/wait-for-it.sh mysql-db:3306  -- echo
echo waiting for mysql init
sleep 3
echo STARTING JAVA
java -Dconfig.file=/app-config/application.conf -jar /app/cromwell.jar server &
pid="$!"
echo JAVA PID IS $pid BASH PID IS $BASHPID DD IS $$
echo processes are
ps -F -A

cleanupFunction() {
    echo "Stopping PID $pid"
    kill -s SIGTERM $pid
    echo "Exiting"
    exit
}

trap cleanupFunction 0 SIGINT SIGTERM SIGUSR1
echo AFTER TRAP
wait 
echo AFTER WAIT
# # A signal emitted while waiting will make the wait command return code > 128
# # Let's wrap it in a loop that doesn't end before the process is indeed stopped
while kill -0 $pid > /dev/null 2>&1; do
 	wait
 	echo WAIT LOOP
 done
echo DONE WAITING


