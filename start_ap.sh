#!/bin/bash

EXIT_LOG_LINES=30
GUNICORN_WORKERS=1
GUNICORN_TIMEOUT=31

echo "Starting CFDE Demo Action Provider";
export FLASK_ENV="development";
rm exit.log;
touch api.log;
truncate --size 0 api.log;
nohup gunicorn --bind 127.0.0.1:5000 --timeout $GUNICORN_TIMEOUT -w $GUNICORN_WORKERS \
    --graceful-timeout $(($GUNICORN_TIMEOUT * 2)) --log-level debug \
    cfde_ap.api:app | tail -n $EXIT_LOG_LINES &>exit.log & disown;
sleep 3;
if [ `cat exit.log` == `cat /dev/null` ]; then
    echo "Action Provider started"
else
    echo "Error starting Action Provider:\n$(cat exit.log)";
fi

