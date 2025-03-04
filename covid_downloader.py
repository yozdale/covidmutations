import argparse
from datetime import date, timedelta
import requests
import numpy
import pandas as pd
import json
import aiohttp
import asyncio

# Sends a request to EBI returning a page of results
async def request_results(session, date, depth, start):
    seq_endpoint = 'https://www.ebi.ac.uk/ebisearch/ws/rest/embl-covid19'
    params = {
        'query': f'collection_date:{date}',
       'fields': 'collection_date,country,host,strain,isolate,region,center_name,lineage,phylogeny,who',
       'size': str(depth),
       'format': 'json',
       'start': str(start)
    }

    try:
        async with session.get(seq_endpoint, params=params) as response :
            if response.status == 200:
                data = await response.text()
                data1 = json.loads(data)
                results_page = pd.json_normalize(data1['entries'])
                print(results_page)
                return results_page
            else:
                print(f'Request failed for date: {date}, position: {start}, with code {response.status_code}')
    except Exception as e:
        print(f'Error with request at date: {date}, position: {start}')
async def process_requests(session, start_date, end_date, depth):
    current_date = start_date
    meta_data = pd.DataFrame()
    while current_date <= end_date:
        delta = timedelta(days=1)
        formatted_date = current_date.strftime('%Y%m%d')
        print(formatted_date)
        results_gathered = 0
        requested_counter = int(depth)
        day_results = pd.DataFrame()

        tasks = []
        while requested_counter > results_gathered:
            subtract_downloading = 100
            if requested_counter - results_gathered >= 100:
                results_gathered += subtract_downloading
            else:
                subtract_downloading = requested_counter - results_gathered
                results_gathered += subtract_downloading
                
            tasks.append(request_results(session, formatted_date, subtract_downloading, results_gathered))
        
        results_list = await asyncio.gather(*tasks)
        for query_results in results_list:
            if results_gathered == 0:
                day_results = query_results
            else:
                day_results = pd.concat([day_results, query_results], ignore_index=True)
            

           
        if meta_data.empty:
            meta_data = day_results
        else:
            meta_data = pd.concat([meta_data,day_results], ignore_index=True)
        current_date += delta

    print(meta_data)

async def main():
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

    async with aiohttp.ClientSession() as session:      
        meta_data = await process_requests(session, start_date, end_date, args.depth)
    print(meta_data)


   






if __name__ == '__main__':
    asyncio.run(main())
