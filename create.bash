#!/bin/bash

dropdb maps
createdb maps
psql maps -f create_db.sql