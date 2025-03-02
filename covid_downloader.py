import argparse
from datetime import date, timedelta
import requests
import numpy
import pandas as pd
import json

# Sends a request to EBI returning a page of results
def request_results(date, depth, start):
    seq_endpoint = 'https://www.ebi.ac.uk/ebisearch/ws/rest/embl-covid19'
    params = {
        'query': f'collection_date:{date}',
       'fields': 'collection_date,country,host,strain,isolate,region,center_name,lineage,phylogeny,who',
       'size': str(depth),
       'format': 'json'
       'start': str(start)
    }
    response = requests.get(seq_endpoint, params)
    print(response.status_code)
    if response.status_code == 200:
        data = response.text
        data1 = json.loads(data)
        results_page = pd.json_normalize(data1['entries'])
        print(results_page)
        return results_page
    
   

def main():
    # Accepts options on the commandline start
    argparser = argparse.ArgumentParser()

    argparser.add_argument('-s','--start_date', help='Input start of time window')
    argparser.add_argument('-e','--end_date', help='Input end of time window')
    argparser.add_argument('-d', '--depth', help='The number of results per day you want to retrieve')
    argparser.add_argument('--updatedb', help='Parses metainformation from files and imports it to the database')
    argparser.add_argument('--storeseq', help='Stores the sequence fasta on hard drive')

    args = argparser.parse_args()
    print(args)
    

    # Iterates through each date in the range and each page of results per day till the depth requiremnt is met, this is all gathered in a pandas dataframe
    start_date = date(int(args.start_date[0:4]), int(args.start_date[4:6]), int(args.start_date[6:8]))
    end_date = date(int(args.end_date[0:4]), int(args.end_date[4:6]), int(args.end_date[6:8]))
    current_date = start_date

    meta_data = pd.DataFrame
    while current_date <= end_date:
        delta = timedelta(days=1)
        formatted_date = current_date.strftime('%Y%m%d')
        print(formatted_date)


        results_gathered = 0
        requested_counter = int(args.depth)
        day_results = pd.DataFrame
        while requested_counter > 0:
            subtract_downloading = requested_counter % 100
            requested_counter = requested_counter - subtract_downloading
            query_results = request_results(formatted_date, subtract_downloading, 0)
            if results_gathered == 0:
                day_results = query_results
            else:
                query_results = pd.concat([day_results, query_results], ignore_index=True)
            results_gathered += subtract_downloading

           
        if current_date == start_date:
            meta_data = day_results
        else:
            meta_data = pd.concat([meta_data,day_results], ignore_index=True)
        current_date += delta

    print(meta_data)


   






if __name__ == '__main__':
    main()
