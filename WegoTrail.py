import os
from google.cloud import bigquery
from unsplash.api import Api
from unsplash.auth import Auth
import json

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="./WegoTrialProject-05a198e9d283.json"

client_id = "ea3d44b5738d41b38e911d2b416ecd6d7e0198f79f860294f3490f35ba9cab8d"
client_secret = "2dd410722fb3aa7bef18e84f3ff70d9855875b622bec63815598c17320dfa73a"
redirect_uri = "urn:ietf:wg:oauth:2.0:oob"
code = ""
            
auth = Auth(client_id, client_secret, redirect_uri, code=code)
api = Api(auth)

def insert(dataset_id, table_id, destinations, links):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    

    table = bigquery_client.get_table(table_ref)
    
    n = 1
    i = 0
    for destination in destinations:
        for link in links[i]:
            jData = json.dumps({'Index': n, 'Destinations': destination, 'Link': 'https://unsplash.com/photos/'+link})
            data = json.loads(jData)
            rows = [data]
            errors = bigquery_client.create_rows(table, rows)
            if not errors:
                print('Loaded 1 row')
            else:
                print('Errors:')
                print(errors)
            n += 1
        i += 1

def getPhoto(city_name):    
    link = list()
    #print(city_name)
    results = api.search.photos(city_name)
    total = results['total']
    photo = results['results']
    try:
        for number in range(0, total):
            #print(photo[number].id)
            link.append(photo[number].id)
            
    except:
        print('')
    
    finally :
        return link
        
def create_table(dataset_id, table_id, project=None):
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Set the table schema
    table.schema = (
        bigquery.SchemaField('Index', 'INTEGER'),
        bigquery.SchemaField('Destinations', 'STRING'),
        bigquery.SchemaField('Link', 'STRING'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))
        
def clear_table():
    client = bigquery.Client()
    query_job = client.query("""DELETE FROM Trial.results WHERE true""")

    results = query_job.result()
    
    
def makeQuery():
    client = bigquery.Client()
    query_job = client.query("""SELECT arrival_city_name, times
    FROM (SELECT arrival_city_name, COUNT (distinct search_id) AS times, RANK() OVER ( ORDER BY (Count(DISTINCT search_id)) DESC ) AS R FROM Trial.flights_searches WHERE created_at >= '2017-08-01 00:00:00' AND created_at < '2017-09-01 00:00:00' GROUP BY arrival_city_name ) as X
    WHERE X.R <= 5""")

    results = query_job.result()
    all_link = list()
    #city = dict()
    city = list()
    
    for row in results:
        print("{}. {}".format(row.arrival_city_name, row.times))
        all_link.append(getPhoto(row.arrival_city_name))
        city.append(row.arrival_city_name)
        #city[row.arrival_city_name] = len(all_link)
        
    #print(all_link)
    insert('Trial', 'results', city, all_link)

#create_table('Trial', 'results')
#clear_table()
makeQuery()
    

    
    
