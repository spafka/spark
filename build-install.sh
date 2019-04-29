#!/usr/bin/env bash

mvn clean install -Dmaven.test.skip=true -Phadoop-2.6 -Pmesos -P!yarn -P!hive  -P!checkstyle -Pscala-2.12 -Dgpg.skip

