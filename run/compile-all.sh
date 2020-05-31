#!/bin/bash
mvn clean compile package install -DskipTests dependency:copy-dependencies