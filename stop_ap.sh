#!/bin/bash

echo "Shutting down Action Provider";
killall -SIGTERM gunicorn;
sleep 3;
if [ `ps -e | grep -c gunicorn` -gt 0 ]; then
    echo "Action Provider still running, please wait and retry"
else
    echo "Action Provider terminated"
fi

