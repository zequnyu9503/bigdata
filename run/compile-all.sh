#!/bin/bash
mvn -f ../ clean compile package install -DskipTests dependency:copy-dependencies