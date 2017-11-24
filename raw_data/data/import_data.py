# -*- coding: utf-8 -*-
"""Import raw data"""

from datetime import datetime
from html_table_parser import HTMLTableParser
from utils import convert_numbers
import commands

def laneHTML(run, path): 
    try:    
        stats_lane = []
        stats_run = []
        epoch = datetime.utcfromtimestamp(0)
        
        date = run.split("_")[0]
        date = "20" + date[0:2] + "-" + date[2:4] + "-" + date[4:6]
        d = datetime.strptime(date, "%Y-%m-%d")
        as_date = (d-epoch).days

        lanehtml = commands.getoutput("find "+ str(path) + str(run) +"/Data/Intensities/BaseCalls/Reports/html/*/all/all/all/ -iname \"lane.html\"")

        with open(lanehtml, "r") as lane:
            html = lane.read()
            tableParser = HTMLTableParser()
            tableParser.feed(html)
            tables = tableParser.tables                         #tables[1]==run tables[2]==lane
            
            stats_run = tables[1][1]
            stats_run = [convert_numbers(item.replace(",", "")) for item in stats_run]
            PCT_PF = (float(stats_run[1])/stats_run[0])*100
            PCT_PF = float("{0:.2f}".format(PCT_PF))
            stats_run.extend([date,as_date,PCT_PF])                
            
            for lane in tables[2][1:]:
                lane = [convert_numbers(item.replace(",", "")) for item in lane]
                stats_lane.append(lane)
        
        return stats_run, stats_lane
    except Exception, e:
        print(repr(e))

def laneBarcodeHTML(run, path):
    try:
        stats_sample = []

        samplehtml = commands.getoutput("find " + str(path) + str(run) + "/Data/Intensities/BaseCalls/Reports/html/*/all/all/all/ -iname \"laneBarcode.html\"")

        with open(samplehtml, "r") as sample:
            html = sample.read()
            tableParser = HTMLTableParser()
            tableParser.feed(html)
            tables = tableParser.tables                         #tables[1]==run tables[2]==sample
            
            for sample_lane in tables[2][1:]:
                if sample_lane[1].upper() != "DEFAULT":
                    stats_sample_lane = [convert_numbers(item.replace(",","")) for item in sample_lane]
                    stats_sample.append(stats_sample_lane)
        
        return stats_sample

    except Exception, e:
        print(repr(e))