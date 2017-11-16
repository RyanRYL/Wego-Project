from google.cloud import bigquery
from unsplash.api import Api
from unsplash.auth import Auth
import json

GOOGLE_APPLICATION_CREDENTIALS = ''
client_id = ''
client_secret = ''
redirect_uri = ''
code = ''
auth = Auth(client_id, client_secret, redirect_uri, code=code)
api = Api(auth)

dataset_id = 'Trial'
table_id = 'results2'

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
                print('This is '+str(n)+'th row')
            else:
                print('Errors:')
                print(errors)
            n += 1
        i += 1
def getPhoto(city_name):
    link = list()
    try:
        results = api.search.photos(city_name)
        total = results['total']
        photo = results['results']
        try:
            if totalt == 0:
                e = 'No photo'
                link.append(e)
            else:
                for number in range(0, total):
                    link.append(photo[number].id)
        except:
            print('')
        finally :
            return link
    except:
        e = 'Exceed the rate limit by hour'
        link.append(e)
       

def create_table(dataset_id, table_id, project=None):
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)
    table.schema = (
        bigquery.SchemaField('Index', 'INTEGER'),
        bigquery.SchemaField('Destinations', 'STRING'),
        bigquery.SchemaField('Link', 'STRING'),
    )
    table = bigquery_client.create_table(table)
#   print('Created table {} in dataset {}.'.format(table_id, dataset_id))

def delete_table(dataset_id, table_id, project=None):
   
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    bigquery_client.delete_table(table_ref)

    print('Table {}:{} deleted.'.format(dataset_id, table_id))
    
def makeQuery():
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = client.query("""SELECT arrival_city_name, CTR, times
    FROM (SELECT arrival_city_name, COUNT (distinct click_id) / COUNT (distinct search_id) AS CTR, COUNT (distinct click_id) AS times, RANK() OVER ( ORDER BY (COUNT (distinct click_id) / COUNT (distinct search_id)) DESC ) AS R FROM `Trial.flights_clicks201708*` GROUP BY arrival_city_name ) as X
    WHERE X.R <= 5 AND X.times >= 20""")
    results = query_job.result()
    all_link = list()
    city = list()
    for row in results:
        print("{}. {}".format(row.arrival_city_name, row.times))
        all_link.append(getPhoto(row.arrival_city_name))
        city.append(row.arrival_city_name)
        
    insert(dataset_id, table_id, city, all_link)
    
delete_table(dataset_id, table_id)
create_table(dataset_id, table_id)
makeQuery()
