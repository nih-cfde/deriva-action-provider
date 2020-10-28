#!/bin/bash

EXIT_LOG_LINES=30
GUNICORN_WORKERS=3
GUNICORN_TIMEOUT=31


if [ "$CONDA_DEFAULT_ENV" == "ap_prod" ]; then
    echo "Starting CFDE Prod VPC Ingest Action Provider";
    export FLASK_ENV="prod";
    rm exit.log;
    touch api.log;
    truncate --size 0 api.log;
    nohup gunicorn --bind 127.0.0.1:5000 --timeout $GUNICORN_TIMEOUT -w $GUNICORN_WORKERS \
        --graceful-timeout $(($GUNICORN_TIMEOUT * 2)) --log-level info \
        cfde_ap.api:app | tail -n $EXIT_LOG_LINES &>exit.log & disown;
    sleep 3;
    if [ `cat exit.log` == `cat /dev/null` ]; then
        echo "Action Provider started"
    else
        echo "Error starting Action Provider:\n$(cat exit.log)";
    fi

elif [ "$CONDA_DEFAULT_ENV" == "ap_staging" ]; then
    echo "Starting CFDE Staging VPC Ingest Action Provider";
    export FLASK_ENV="staging";
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

elif [ "$CONDA_DEFAULT_ENV" == "ap_dev" ]; then
    echo "Starting CFDE Dev VPC Ingest Action Provider";
    export FLASK_ENV="dev";
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

else
    echo "CONDA_DEFAULT_ENV '$CONDA_DEFAULT_ENV' invalid!";
    echo "Expecting ap_prod, ap_staging, or ap_dev.";
fi
