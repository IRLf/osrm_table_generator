#this is a fork of the code found on https://iliauk.wordpress.com/2016/02/23/millions-of-distances-osrm-python/
#adapted to linux (specifically ubuntu in the linux for windows subsystem)
#and for OSRM July 2019 release
#significant changes done by Rick Fahey
#this is kind of dangerous right now since it doesn't check to see if a server is running
#nor does it close a server when done
#this was impractical for me working with a server that loaded 20GB into memory
#original code was for windows

import os
import requests
import json
import time
from tqdm import tqdm
import csv
import pandas as pd
import argparse
import sys
#from multiprocessing import *
#from subprocess import Popen, PIPE

 
#initiate the parser 
parser = argparse.ArgumentParser(description="This script takes in a CSV formatted as ID number (qid), Start Latitude (alat), Start Longitude (alon),\
                                                End Latitude (blat), End Longitude (blon) and generates an output file with times to the destinations.\
                                                It requires an active OSRM server to generate the data.\
                                                To make file processing faster, it is recommended that the server be started\
                                                in a non-verbose mode and that --max-table_size is large enough to handle the data")
required = True
#add Long and short arguments

parser.add_argument("-i", "--input", help="location of input csv file, default is osrm_input.csv in working directory", type = str, required=False)
parser.add_argument("-o", "--output", help="output file and location, default is osrm_output.csv in working directory", type = str, required = False)
parser.add_argument("-s", "--server", help="server location, default is localhost", type = str, required = False)
parser.add_argument("-p", "--port", help="define the port that the server is operating on, default is 5000", type = int, required=False)
parser.add_argument("-d", "--Directory", help="Log directory, default is working directory", type = str, required=False)
#parser.add_argument("-m", "--Map", help='Location of .OSRM Map file, required if using script to start server', type = str, required=True)
parser.add_argument("-t", "--transport", help = "Define what transportation method to use for the map. e.g. car", type = str, required = False)

args=parser.parse_args()

if not args.input:
    print "No input defined"
    print "Setting input to ./osrm_input.csv"
    args.input = "./osrm_input.csv"
else:
    print "Input file is: {0}" .format(args.input)

if not args.output:
    print "No output defined"
    print "Setting output to ./osrm_output.csv"
    args.output = "./osrm_output.csv"
else:
    print "Output file is: {0}" .format(args.output)

if not args.server:
    print "No server defined"
    print "Setting server to Localhost"
    args.server = "localhost"
else:
    print "Server located at: {0}" .format(args.server)

if not args.port:
    print "No port defined"
    print "Setting port to 5000"
    args.port = 5000
else:
    print "Server Port is: {0}" .format(args.port)

if not args.Directory:
    print "Working Directory not specified"
    print "Local Directory will be Used"
    args.Directory = "./"
else:
    print "Working Directory: {0}" .format(args.Directory)

# if not args.Map:
    # print "No map file defined"
    # print "Using default map, map.osm"
    # args.Map = "map.osm"
# else:
    # print "Map: {0}" .format(args.Map)

if not args.transport:
    print "No Transportation method defined"
    print "Setting Default to car"
    args.transport = "car"
else:
    print "Transportation method: {0}" .format(args.transport)

 
# class OsrmEngine(object):
    # """ Class which connects to an osrm-routed executable and spawns multiple servers"""
    # def __init__(self, map_loc, router_loc, max_size):
        # """
        # Map needs to be pre-processed with osrm-prepare; router_loc should be the most up to date file from here:
        # http://build.project-osrm.org/ - both can work over the network with no significant speed-fall as they
        # are initially loaded into RAM
        # """
        # if not os.path.isfile(router_loc):
            # raise Exception("Could not find osrm-routed executable at: %s" % router_loc)
        # else:
            # self.router_loc = router_loc
        # if not os.path.isfile(map_loc):
            # raise Exception("Could not find osrm network data at: %s" % map_loc)
        # else:
            # self.map_loc = map_loc
        # #Remove any open instance
        # if self.server_check():
            # self.server_kill()
 
    # def server_kill(self):
        # # """
        # # Kill any osrm-routed server that is currently running before spawning new - this means only one script
        # # can be run at a time
        # # """
        # Popen('taskkill /f /im %s' % os.path.basename(self.router_loc), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        # time.sleep(2)
 
    # def server_check(self):
        # """ 
        # Check if server is already running
        # """
        # try:
            # if requests.get("http://%s:%d" % (ghost, gport)).status_code == 400:
                # return True
        # except requests.ConnectionError:
            # return False
 
    # def server_start(self):
        # """
        # Robustness checks to make sure server can be initialised
        # """
        
        # print("Entering Server Start")
        
        # try:
             # p = Popen([self.router_loc, '-v'], stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
             # output = p.communicate()[0].decode("utf-8")
        # except:
        # # except FileNotFoundError:
             # output = ""
        # if "v" not in str(output):
            # raise Exception("OSM does not seem to work properly")
        # # Check server not running
        # if self.server_check():
            # raise Exception("osrm-routed already running - force close all with task-manager")
        # # Start server
        # Popen("%s %s -i %s -p %d -t %d" % (self.router_loc, self.map_loc, args.server, args.server, cpu_count()), stdout=PIPE, shell=True)
        # #
        # #time.sleep(90)
        # c = 0
        # while c < 30:
            # print("Trying to Start Stuff")
            # try:
                # if requests.get("http://%s:%d" % (args.server, args.server)).status_code == 400:
                    # return "http://%s:%d" % (args.server, args.server)
                # else:
                    # raise Exception("Map could not be loaded")
            # except requests.ConnectionError:
                    # time.sleep(10)
                    # c += 1
        # raise Exception("Map could not be loaded ... taking more than 5 minutes..")
 
    # def spawn_server(self):
        # """ Server can handle parallel requests so only one instance needed """
        # print("------------------starting server----------------")
        # p = Process(target=self.server_start)
        # p.start()
        # p.join()
 
 
# Helper functions
def process_request(url_input,routelist):
    """
    Submits HTTP request to server and returns distance metrics; errors are coded as status=999
    """
    print("Processing URLs")
    req_url = url_input
    
    routelistlen = len(routelist)
    
    try_c = 0
    tot_time_s = [0]*routelistlen
    tot_time_m = [0]*routelistlen

    out = [[0]*6 for i in range(routelistlen)]
    
    while try_c < 5:

        try:
            response = requests.get(req_url)
            json_geocode = response.json()
            status = json_geocode['code']
            print "Processing JSON file"
            if status in 'Ok':
            #if True:
                for x in tqdm(range(routelistlen)):
                    tot_time_s[x] = json_geocode['durations'][0][x]
                    
                    tot_time_m[x] = tot_time_s[x]/60
                    
                # tot_dist_m = json_geocode['routes'][0]['distance']
                # #converting the miles calculated
                # tot_dist_miles = tot_dist_m*0.000621371
                    #print ("{}% complete".format(((x*100)/(routelistlen-1))))
                    out[x][0] = routelist.iloc[x]['qid']
                    out[x][1]= tot_time_m[x]
                    out[x][2] =json_geocode['sources'][0]['location'][0]
                    out[x][3] =json_geocode['sources'][0]['location'][1]
                    out[x][4] =json_geocode['destinations'][x]['location'][0]
                    out[x][5] =json_geocode['destinations'][x]['location'][1]
                return out
                # Cannot find route between points (code errors as 999)
            else:
                return [0, 999, 0, 0, 0, 0, 0, 0]
        except Exception as err:
            print("%s - retrying..." % err)
            print("error should be ^")
            time.sleep(5)
            try_c += 1
    print("Failed: %d %s" % (query_id, req_url))
    return [query_id, 999, 0, 0, 0, 0, 0, 0]
 
 
def create_urls(routes, ghost, gport):
    """ Python list comprehension to create URLS """
        
    return [["http://{0}:{1}/route/v1/car/{2},{3};{4},{5}".format(
        ghost, gport, alon, alat, blon, blat), qid] for qid, alat, alon, blat, blon in routes]
        
def create_group_url(routelist, ghost, gport):
    # creating one big url to parse in a table real quick
    host = "http://{0}:{1}/".format(ghost, gport)
    path = "table/v1/car/"

    waypoints = [[0]*2 for i in range(len(routelist)+1)]
    waypoints[0][0] = routelist.iloc[0]['alat']
    waypoints[0][1] = routelist.iloc[0]['alon']
    
    print "Making URL"
    for x in tqdm(range(len(routelist))):
        waypoints[x+1][0] = routelist.iloc[x]['blat']
        waypoints[x+1][1] = routelist.iloc[x]['blon']
    waypointlist = ';'.join(map(lambda pt: '{},{}'.format(*reversed(pt)), waypoints))
    return "{}{}{}?sources=0".format(host,path,waypointlist)
 
def loadroute_csv(csv_loc):#, chunks):
    """ Use Pandas to iterate through CSV - very fast CSV parser """
    if not os.path.isfile(csv_loc):
        raise Exception("Could not find CSV with addresses at: %s" % csv_loc)
    else:
        return pd.read_csv(csv_loc, sep=',')
        
 
if __name__ == '__main__':
    try:
        stime = time.time()
        # Router_loc points to latest build http://build.project-osrm.org/
        router_loc = '/home/rick/osrm-backend/build/osrm-routed' #'.../osrm-routed.exe'
        # Directory containing routes to process (csv) and map supplied as arg.
        # if args.Map:    
            # directory_loc = os.path.normpath(args.Directory)
            # map_loc = os.path.normpath(args.Map)
            # print("Initialising engine")
            # osrm = OsrmEngine(map_loc, router_loc)
            # print("Loading Map - this may take a while")
            # osrm.spawn_server()
        done_count = 0
        print("Loading CSV")
        with open(os.path.join(directory_loc, args.output), 'w') as outfile:
            wr = csv.writer(outfile, delimiter=',', lineterminator='\n')
            
            #preparing the CSV Columns
            wr.writerow(['query_id', 'total time', 'Start Longitude', 'Start Latitude', 'Destination Longitude', 'Destination Latitude'])
            routelist = loadroute_csv(csv_loc=os.path.join(directory_loc, args.input))#, chunks=args.chunks)
            bigaddress = create_group_url(routelist,args.server,args.server)
                        
            calc_routes = process_request(bigaddress,routelist)
            print("Saving Progress")
            wr.writerows(calc_routes)
            dur = time.time() - stime

        print("---------------------")
        print("Process Completed")
        print("It took {0} seconds to complete {1} calculations".format(dur, len(routelist)))
        print("---------------------")
        #osrm.server_kill()
    except Exception as err:
        print(err)
        #osrm.server_kill()
        time.sleep(15)
