import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import playercareerstats
import requests
from bs4 import BeautifulSoup
import re
from alive_progress import alive_bar
from datetime import datetime
import boto3
import io
from io import StringIO


def run_etl():
    nba_players = players.get_players()
    players_df = pd.DataFrame.from_dict(nba_players)
    players_df = players_df.head(500)
    dobs = []
    country = []
    with alive_bar(len(players_df)) as bar:
        
        for ind, row in players_df.iterrows():
            base_url = "https://www.nba.com/stats/player/"
            url = base_url + str(row['id']) +"/shooting"
            page = requests.get(url)
            soup = BeautifulSoup(page.content, "html.parser")
            infobox = soup.find('div', {'class': 'PlayerSummary_statsDetails__FRW2E'})
            if infobox:
                p_vals = []
                for p in infobox.find_all('p'):
                    p_vals.append(p.text)

                dob = p_vals[4]
                cntry = p_vals[6]
            else:
                dob,cntry = None, None
            
            dobs.append(dob)
            country.append(cntry)
            bar()
        
    players_df['DOB'] = dobs
    players_df['Country'] = country
    
    df = players_df
    pattern = r"\b(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{1,2},\s\d{4}\b"
    
    filtered_df = df[df["DOB"].str.contains(pattern)]
    filtered_df["Birth_date"] = pd.to_datetime(df['DOB'],format='%B %d, %Y')
    
    # Finding the age of the players
    current_date = datetime.now()
    filtered_df["Age"] = current_date.year - filtered_df['Birth_date'].dt.year
    
    # Dropping the DOB column
    
    filtered_df.drop(columns=["DOB"],inplace=True)
    filtered_df.to_csv('transformed_data.csv',sep=',',index=False,encoding='utf-8')
    
    csv_buffer = StringIO()
    filtered_df.to_csv(csv_buffer, index=False)
    s3_client = boto3.client('s3')
    
    bucket_name = "firsts3bucket-ksv"
    object_name = "test_df.csv"
    
    response = s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=csv_buffer.getvalue())

run_etl()