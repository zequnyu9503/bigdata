#!/bin/bash
git pull
mvn clean compile package install -DskipTests dependency:copy-dependencies