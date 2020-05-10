#this is a fork of the code found on https://iliauk.wordpress.com/2016/02/23/millions-of-distances-osrm-python/
#and for OSRM May 2020 release
#changes done by Rick Fahey
#this is kind of dangerous right now since it doesn't check to see if a server is running
#nor does it close a server when done
#this was impractical for me working with a server that loaded 20GB into memory
#tested only using linux subsystem for windows and linux OSRM server

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
#parser.add_argument("-c", "--chunks", help = "Define how large of a chunk size to process.", type = int, required = False)

args=parser.parse_args()

if not args.input:
    print("No input defined")
    print("Setting input to ./osrm_input.csv")
    args.input = "./osrm_input.csv"
else:
    print(("Input file is: {0}").format(args.input))

#if not args.chunks:
#    print("No chunk size defined")
#    print("Setting chunk size to 50,000")
#    args.chunks = 50000
#else:
#    print("Chunk size is: {0}") .format(args.chunks)

if not args.output:
    #print("No output defined")
    print("Setting output to ./osrm_output.csv")
    args.output = "./osrm_output.csv"
    args.output = None
else:
    print(("Output file is: {0}").format(args.output))

if not args.server:
    print("No server defined")
    print("Setting server to Localhost")
    args.server = "localhost"
else:
    print("Server located at: {0}" .format(args.server))

if not args.port:
    print("No port defined")
    print("Setting port to 5000")
    args.port = 5000
else:
    print("Server Port is: {0}" .format(args.port))

if not args.Directory:
    print("Working Directory not specified")
    print("Local Directory will be Used")
    args.Directory = "./"
else:
    print("Working Directory: {0}" .format(args.Directory))

# if not args.Map:
    # print"No map file defined"
    # print"Using default map, map.osm"
    # args.Map = "map.osm"
# else:
    # print"Map: {0}" .format(args.Map)

if not args.transport:
    print("No Transportation method defined")
    print("Setting Default to car")
    args.transport = "car"
else:
    print(("Transportation method: {0}").format(args.transport))

# Helper functions
def process_request(url_input,routelist):
    """
    Submits HTTP request to server and returns distance metrics; errors are coded as status=999
    """
    print("Processing URLs")
    #req_url = url_input
    
    routelistlen = len(routelist)
    
    try_c = 0
    tot_time_s = [0]*routelistlen
    tot_time_m = [0]*routelistlen

    out = [[0]*6 for i in range(routelistlen)]
    
    while try_c < 5:

        try:
            #response = requests.get(req_url)
            response = requests.get(url_input)
            json_geocode = response.json()
            status = json_geocode['code']
                        
            #jsonout = open("json_out.txt","w")
            #jsonout.write(str(json_geocode))
            #jsonout.close()
            #print("json file saved")
            
            print("Processing JSON file")
            print("JSON Status: ", status)
            if status in 'Ok':
                columns = {'query_id':[0],'total_time':[0],'Start_Longitude':[0], 'Start_Latitude':[0]\
                           ,'Destination_Longitude':[0],'Destination_Latitude':[0]}
                
                #out = pd.DataFrame(data=columns)
                dict = {}   #making a dictionary to pass stuff to pandas because pandas append is REALLY SLOW
                for x in tqdm(range(routelistlen)):
                    tot_time_s[x] = json_geocode['durations'][0][x]
                    
                    if tot_time_s[x] == None:
                        tot_time_s[x] = 0

                    tot_time_m[x] = tot_time_s[x]/60

                    #append method for pandas
                    dict[x] = {'query_id':x,
                                      'total_time':tot_time_m[x],
                                      'Start_Longitude':json_geocode['sources'][0]['location'][0],
                                      'Start_Latitude':json_geocode['sources'][0]['location'][1],
                                      'Destination_Longitude':json_geocode['destinations'][x]['location'][0],
                                      'Destination_Latitude':json_geocode['destinations'][x]['location'][1]}
                    
                    #out = out.append({'query_id':x,
                    #                  'total_time':tot_time_m[x],
                    #                  'Start_Longitude':json_geocode['sources'][0]['location'][0],
                    #                  'Start_Latitude':json_geocode['sources'][0]['location'][1],
                    #                  'Destination_Longitude':json_geocode['destinations'][x]['location'][0],
                    #                  'Destination_Latitude':json_geocode['destinations'][x]['location'][1]},
                    #                  ignore_index=True)
                
                #reading in out file from dict
                out = pd.DataFrame.from_dict(dict,"index")

                #removing 0 time points
                print("Removing 0 Time Locations")

                out = out[out.total_time != 0]

                out = out[['query_id','total_time','Start_Longitude', 'Start_Latitude','Destination_Longitude','Destination_Latitude']]
                
                return out
                # Cannot find route between points (code errors as 999)
            else:
                print("Status NOT OK")

                return out
        except Exception as err:
            print("%s - retrying..." % err)
            print("Status Says:",status)
            print("Iteration: ",x)
            print("Data from JSON:")
            print("Latitude: ",out[x-1][5])
            print("Longitude: ",out[x-1][4])
            
            time.sleep(5)
            try_c += 1
    print("Failed: ",x)
    return out
 
 
def create_urls(routes, ghost, gport):
    """ Python list comprehension to create URLS """
        
    return [["http://{0}:{1}/route/v1/car/{2},{3};{4},{5}".format(
        ghost, gport, alon, alat, blon, blat), qid] for qid, alat, alon, blat, blon in routes]
        
def create_group_url(routelist, ghost, gport):
    # creating one big url to parse in a table real quick
    host = "http://{0}:{1}/".format(ghost, gport)
    path = "table/v1/car/"

    waypoints = [[0]*2 for i in range(len(routelist)+1)]
    waypoints[0][0] = routelist.at[0,'alat']
    waypoints[0][1] = routelist.at[0,'alon']
    
    print("Making URL")
    for x in tqdm(range(len(routelist))):
        waypoints[x+1][0] = round(routelist.at[x,'blat'],6)
        waypoints[x+1][1] = round(routelist.at[x,'blon'],6)
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
        directory_loc = args.Directory
        done_count = 0
        print("Loading CSV")
        #with open(os.path.join(directory_loc, args.output), 'w') as outfile:
            #wr = csv.writer(outfile, delimiter=',', lineterminator='\n')
            
            #preparing the CSV Columns
            #wr.writerow(['query_id', 'total time', 'Start Longitude', 'Start Latitude', 'Destination Longitude', 'Destination Latitude'])
        routelist = loadroute_csv(csv_loc=os.path.join(directory_loc, args.input))#, chunks=args.chunks)
        bigaddress = create_group_url(routelist,args.server,args.port)
        
        #bigadd = open("address.txt","w")
        #bigadd.write(bigaddress)
        #bigadd.close()
        
        calc_routes = process_request(bigaddress,routelist)
        print(type(calc_routes))
        print("Saving Progress")
        calc_routes.to_csv(os.path.join(directory_loc, args.output), index=False)

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
