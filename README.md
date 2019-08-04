# osrm_table_generator
Python Script to process a location-location time table via OSRM server and CSVs
This script takes in a CSV formatted as:
ID number (qid), Start Latitude(alat), Start Longitude (alon), End Latitude (blat), End Longitude (blon)
and generates an output file with times to the destinations. It requires an OSRM
server to generate the data.

It is recommended to run the OSRM server in a non-verbose mode and that the --max-table-size be set to or larger
than the data set you're inputting, which will prevent errors generated from the server refusing larger table sizes
