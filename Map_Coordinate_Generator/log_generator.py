#!/usr/bin/python3
#coding=UTF-8

import random
import time

locations = [
    # "33.646011,-117.842918",   # Aldrich Park
    # "33.643359,-117.841965",   # ICS
    # "33.656971,-117.831673",   # William R Mason Regional Park
    # "33.650591,-117.839565",   # UTC
    # "33.649219,-117.848359",   # Bren Events Center
    # "33.645936,-117.855907",   # University Research Park
    # "33.640819,-117.840599"    # University Hills
    "33.81050175757576,-117.99366339759037",
    "33.78106690909091,-117.93783178313254",
    "33.74574509090909,-117.86730763855422",
    "33.73397115151515,-117.98778638554218",
    "33.686875393939395,-117.83204556626507",
    "33.68393190909091,-117.91138522891566",
    "33.63094918181818,-117.60578060240964",
    "33.61917524242424,-117.71156681927711",
    "33.698649333333336,-117.76445992771085",
    "33.70747978787879,-117.94370879518073"
]

phones = [
    "7147217231", "7147323933", "2345830213", 
    "5634243523", "4520194836", "8834591276",
    "3484325089", "1235634567", "2456645774"
]

def sample_phone():
    return random.sample(phones, 1)[0]

def sample_location():
    return random.sample(locations, 1)[0]

def sample_latency():
    return random.randint(1, 100)

def generate_log(count = 10):
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    f = open("/Users/haikangchen/Programs/Storm/logs/access.log", "a+")
    while count >= 1:
        query_log = "{phone}\t{location}\t[{local_time}]\t{latency}".format(phone=sample_phone(), local_time = time_str, location = sample_location(), latency = sample_latency())
        # print (query_log)
        f.write(query_log + "\n")
        count = count - 1

if __name__ == "__main__":
    generate_log(500)