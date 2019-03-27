#!/usr/bin/env python
# coding: utf-8
import sys
import os
import time

try:
    cur_linkid = ""
    cur_linkcity = ""
    cur_hour = ""
    cur_time_str = ""
    cur_confidence = ""
    cur_speed = 0.0

    last_linkid = ""
    last_linkcity = ""
    last_hour = ""
    last_time_str = ""
    last_confidence = ""
    last_speed = 0.0

    all_hour_speed_list = []
    hour_ave_speed_list = []

    is_first_line = 1
    #parse mappered info
    for _line in sys.stdin:
        line = _line.strip()
        items = line.split(",")
        if len(items) != 6:
            continue

        if is_first_line == 1:
            is_first_line = 0

            cur_linkid = items[0].strip()
            cur_linkcity = items[1].strip()
            cur_hour = items[2].strip()
            cur_time_str = items[3].strip()
            cur_confidence = items[4].strip()
            cur_speed = float(items[5].strip())

            all_hour_speed_list.append(cur_speed)
        else:
            last_linkid = cur_linkid
            last_linkcity = cur_linkcity
            last_hour = cur_hour
            last_time_str = cur_time_str
            last_confidence = cur_confidence
            last_speed = cur_speed

            cur_linkid = items[0].strip()
            cur_linkcity = items[1].strip()
            cur_hour = items[2].strip()
            cur_time_str = items[3].strip()
            cur_confidence = items[4].strip()
            cur_speed = float(items[5].strip())

            #same linkid
            if last_linkid == cur_linkid:
                #same linkid same hour
                if last_hour == cur_hour:
                    all_hour_speed_list.append(cur_speed)
                #same linkid diff hour
                else:
                    if len (all_hour_speed_list) >= 4:
                        hour_ave_speed = sum(all_hour_speed_list) / len(all_hour_speed_list)
                        hour_ave_speed_list.append(hour_ave_speed)
                    all_hour_speed_list = []
                    all_hour_speed_list.append(cur_speed)
            #diff linkid
            else:
                if len (all_hour_speed_list) >= 4:
                    hour_ave_speed = sum(all_hour_speed_list) / len(all_hour_speed_list)
                    hour_ave_speed_list.append(hour_ave_speed)
                all_hour_speed_list = []
                all_hour_speed_list.append(cur_speed)
                
                #calc and print link free_flow_speed and free_time
                hour_ave_speed_list.sort()
                if (len(hour_ave_speed_list) > 0) and (len(hour_ave_speed_list) <= 4):
                    link_max_speed = hour_ave_speed_list[(len(hour_ave_speed_list) - 1)]
                    link_min_speed = hour_ave_speed_list[0]
                    link_free_flow_speed = sum(hour_ave_speed_list) / len(hour_ave_speed_list)
                    
                    print("%s,%f" % (last_linkid, link_free_flow_speed))
                elif len(hour_ave_speed_list) > 4:
                    tmplist = hour_ave_speed_list[(len(hour_ave_speed_list) - 4):]
                    link_max_speed = tmplist[(len(tmplist) - 1)]
                    link_min_speed = tmplist[0]
                    link_free_flow_speed = sum(tmplist) / len(tmplist)
                    
                    print("%s,%f" % (last_linkid, link_free_flow_speed))
                hour_ave_speed_list = []

    if len (all_hour_speed_list) >= 4:
        hour_ave_speed = sum(all_hour_speed_list) / len(all_hour_speed_list)
        hour_ave_speed_list.append(hour_ave_speed)
    all_hour_speed_list = []

    hour_ave_speed_list.sort()
    if (len(hour_ave_speed_list) > 0) and (len(hour_ave_speed_list) <= 4):
        link_max_speed = hour_ave_speed_list[(len(hour_ave_speed_list) - 1)]
        link_min_speed = hour_ave_speed_list[0]
        link_free_flow_speed = sum(hour_ave_speed_list) / len(hour_ave_speed_list)

        print("%s,%f" % (last_linkid, link_free_flow_speed))
    elif len(hour_ave_speed_list) > 4:
        tmplist = hour_ave_speed_list[(len(hour_ave_speed_list) - 4):]
        link_max_speed = tmplist[(len(tmplist) - 1)]
        link_min_speed = tmplist[0]
        link_free_flow_speed = sum(tmplist) / len(tmplist)

        print("%s,%f" % (last_linkid, link_free_flow_speed))
    hour_ave_speed_list = []

except Exception, e:
    print("Exception, :", str(e.message))
    exit(1)
