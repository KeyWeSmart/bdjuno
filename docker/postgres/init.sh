#!/bin/bash

psql -U postgres -h localhost -c "CREATE DATABASE \"sesame\";"
psql -U postgres -h localhost -c "GRANT ALL PRIVILEGES ON DATABASE \"sesame\" TO \"postgres\";"