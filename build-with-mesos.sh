#!/usr/bin/env bash
./dev/make-distribution.sh --tgz --mvn mvn  -Phive -Pyarn -Pmesos -Pscala-2.12  -DskipTests

