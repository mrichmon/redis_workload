#!/bin/bash

cat ${1} | grep run2 | grep ${2} | cut -d : -f 2 | cut -c 2-
