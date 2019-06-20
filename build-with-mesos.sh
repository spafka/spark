#!/usr/bin/env bash
./dev/make-distribution.sh --tgz --mvn mvn  -Phive -Pyarn -Pmesos  -Phadoop-2.7 -Pscala-2.12  -DskipTests

