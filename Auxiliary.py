import datetime
import logging
import configparser
import os

def parse_norwegian_date(s):

    month_map = {
        "jan.": 1, "januar": 1,
        "feb.": 2, "februar": 2,
        "mars": 3,
        "apr.": 4, "april": 4,
        "mai": 5,
        "juni": 6,
        "juli": 7,
        "aug.": 8, "august": 8,
        "sep.": 9, "sept.": 9, "september": 9,
        "okt.": 10, "oktober": 10,
        "nov.": 11, "november": 11,
        "des.": 12, "desember": 12
    }    

    parts = s.split()

    day = int(parts[0].replace(".", ""))
    month = month_map[parts[1]]
    year = int(parts[2])

    hour, minute = parts[3].split(":")
    hour = int(hour)
    minute = int(minute)

    return datetime.datetime(year, month, day, hour, minute)

def initialise():

    #Set basic configuration for logging
    logging.basicConfig(filename="".join((os.path.dirname(os.path.realpath(__file__)), '/logfile.log')), encoding='utf-8', format='%(levelname)s: %(asctime)s - %(message)s', datefmt='%m-%d-%Y %I:%M:%S', level=logging.WARNING)

    #get data from INI file
    configData = configparser.ConfigParser()
    with open("".join((os.path.dirname(os.path.realpath(__file__)), '/config.ini')),"r") as configFile:
        configData.read_file(configFile)
        useFile = configData.get("URLfile", "usefile")
        URLfile = configData.get("URLfile", "urlfile")
        start = int(configData.get("finnkoder", "start"))
        finish = int(configData.get("finnkoder", "end"))
        continueProgression = configData.get("progress", "continue")
        progression = int(configData.get("progress", "progression"))
    configFile.close()

    #Set start point to where the program stopped last run
    if (continueProgression == "True"):
        start = progression

    #Get all URLs from URL file
    with open(URLfile, 'r') as file:
        ad_list = []
        for i in file.readlines():
            i = i.rstrip('\r\n')
            ad_list.append(i)

    return useFile, ad_list, start, finish, configData

def appendProgress(configData, useFile, URL, i):
    try:
        #Write progression to INI file
        if (useFile == "False"):
            configData["progress"]["progression"] = str(i)
        configData["progress"]["currentURL"] = URL

        with open("".join((os.path.dirname(os.path.realpath(__file__)), '/config.ini')), 'w') as conf:
            configData.write(conf)
    except Exception as e:
        logging.error(i, " - ", "Error getting data form ini file or URL file. Exception thrown: ", str(e))

