#!/usr/bin/env python
# coding: utf-8
import sys
import os
import time

def time_str2hour(time_str):
    hour = time_str[0:3]
    return int(hour)

try:
    #parse traffic history info
    for _line in sys.stdin:
        line = _line.strip()
        items = line.split(",")
        if len(items) < 9:
            continue
        time_str = items[1].strip()
        if len(time_str) != 6:
            continue
        hour = time_str2hour(time_str)
        linkid = items[2].strip()
        confidence = items[6].strip()
        #filter link traffic confidence(0~100) below 20
        if float(confidence) < 20:
            continue

        speed = items[5].strip()
        if speed == "0.0":
            continue

        print("%s,%s,%d,%s,%s,%s" % (linkid, "0", hour, time_str, confidence, speed))

except Exception, e:
    print("Exception :", str(e.message))
    exit(1)
