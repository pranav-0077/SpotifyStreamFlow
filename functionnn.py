import json
import boto3
from datetime import datetime
from io import StringIO
import pprint
import pandas as pd
def album(data):
    album_list=[]
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id':album_id,'name':album_name,'release_date':album_release_date,
                        'total_tracks':album_total_tracks,'url':album_url}

import boto3
from datetime import datetime
from io import StringIO
import pprint
import pandas as pd
def album(data):
    album_list=[]
    for row in data['items']:
        album_id = row['track']['album']['id']
…            s3_resource.meta.client.copy(copy_source, bucket_name, 'raw_data/processed_data/'+key.split('/')[-1])
            s3.delete_object(Bucket=bucket_name, Key=key)
        album_list.append(album_element)
    return album_list
def art(artist_data):
    artist_list=[]
    for row in artist_data['items']:
        print(row)
        for artist in row['track']['artists']:
                artist_dict = {'artist_id':artist['id'], 'artist_name':artist['name'], 'external_url': artist['href']}
                artist_list.append(artist_dict)
    return artist_list
def song(song_data):
    song_list=[]
    for row in song_data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,
                            'popularity':song_popularity, 'song_added':song_added}
        song_list.append(song_element)
    return song_list


def lambda_handler(event, context):
    s3=boto3.client("s3")
    bucket_name='spotifybucket000'
    file_name='raw_data/to_processed/'
    spotify_data=[]
    spotify_key=[]

    for file in s3.list_objects(Bucket=bucket_name,Prefix=file_name)['Contents']:
       print(file)
       file_key=file['Key']
       if file_key.split('.')[-1]=='json':
        data=s3.get_object(Bucket=bucket_name, Key=file_key)
        print(data)
        res=data['Body']
        print(res)
        json_data=json.loads(res.read())
        print(json_data)
        spotify_data.append(json_data)
        spotify_key.append(file_key)
        print(spotify_data)
        print(spotify_key)
    for data in spotify_data:
        album_list=album(data)
        print(album_list)
        album_df=pd.DataFrame(album_list)
        album_df=album_df.drop_duplicates(subset=['album_id'])
        print(album_df)
        album_df['release_date']=pd.to_datetime(album_df['release_date'],format='ISO8601')
        print(album_df)
        album_key = 'transformed_data/album_data/album_transformed_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.json'
        album_buffer=StringIO()
        album_df.to_csv(album_buffer)
        album_content=album_buffer.getvalue()
        s3.put_object(Bucket=bucket_name, Key=album_key, Body=album_content)
        s3_resource=boto3.resource('s3')
      
    for artist_data in spotify_data:
        artist_list=art(artist_data) 
        print(artist_list)  
        artist_df=pd.DataFrame(artist_list)
        artist_df=artist_df.drop_duplicates(subset=['artist_id'])
        artist_key='transformed_data/artist_data/artist_transformed_'+ datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.json'
        artist_buffer=StringIO()
        artist_df.to_csv(artist_buffer)
        artist_content=artist_buffer.getvalue()
        s3.put_object(Bucket=bucket_name, Key=artist_key, Body=artist_content)
        s3_resource=boto3.resource('s3')
     
    
    for song_data in spotify_data:
        song_list=song(song_data)
        print(song_list)
        song_df=pd.DataFrame(song_list)
        song_df['song_added']=pd.to_datetime(song_df['song_added'], format='ISO8601')
        song_key='transformed_data/songs_data/songs_transformed_'+ datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.json'
        song_buffer=StringIO()
        song_df.to_csv(song_buffer)
        song_content=song_buffer.getvalue()
        s3.put_object(Bucket=bucket_name, Key=song_key, Body=song_content)
        s3_resource=boto3.resource('s3')
        for key in spotify_key:
            copy_source={'Bucket':bucket_name,'Key':key}
            s3_resource.meta.client.copy(copy_source, bucket_name, 'raw_data/processed_data/'+key.split('/')[-1])
            s3.delete_object(Bucket=bucket_name, Key=key)
