# nord-stream

Project to analyze AIS data from Denmark to review ships near the NS1 and NS2 pipeline explosions
Data: https://web.ais.dk/aisdata/

Note, the data source is very slow and downloading data efficiently requires a tool like aria2.
(usage `c -x 16 https://web.ais.dk/aisdata/aisdk-2022-06.zip`)

Files:
- algo.py: Contains quick functions to determine if points fall in the NS1 or NS2 explosion boundary boxes
- convert_ais.py: Take an AIS export and filter to tracts within the NS1&NS2 boundary boxes and export the data

To do:
- Adapt convert_ais.py to process a full month of data (each day is provided as a 500MB file)
