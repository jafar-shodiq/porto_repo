from elasticsearch import Elasticsearch
import elasticsearch.helpers as es_helper
import pandas as pd
from dotenv import load_dotenv
import warnings
warnings.filterwarnings("ignore")

load_dotenv()

def connect_elasticsearch_read():
    """
    connecting elasticsearch.
    to check whether it's connected or not: es.info().
    expected result: matched cluster name.
    """
    
    es = Elasticsearch(
        ["https://myapi.co.id:xxx"],
        api_key=os.getenv("ENV_KEY"),
        verify_certs=False
    )
    
    return es

def get_data(field1_value, scroll_minute=10, scroll_size=1000):
    """
    get data from elasticsearch with python client.
    must connect with host first.
    expected result: elasticsearch data in a pandas dataframe format.
    """
    
    es = connect_elasticsearch_read()
    
    query = {
        "_source": ["@timestamp", "field1", "field2"],
        "query": {
          "bool": {
            "must": [
              {
                "term": {
                  "field1.keyword": {
                    "value": f"{field1_value}"
                  }
                }
              }
            ]
          }
        }
      }, 
      "track_total_hits": "true"
    }

    result = es_helper.scan(es, query=query, index=["logstash-mydata"], scroll=f"{scroll_minute}m", size=scroll_size)
    final_result = list(result)
    
    df = pd.json_normalize(final_result)

    return df

df = get_data("ID12345")
print(df)




















