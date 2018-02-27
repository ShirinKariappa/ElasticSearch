from elasticsearch import Elasticsearch
import pandas as pd
import json

df = pd.read_csv(filename,sep=',')
es = Elasticsearch('http://ip:port/')
#es = Elasticsearch()

es.indices.refresh(index="index_name")

def search(search_query,category,type_):
    result=[]
    query={
  "query": {
    "multi_match" : {
  "query":search_query,
      "type":       "most_fields",
      "fields": ["category","summary_of_incident","type","item","details"],
      
        "fuzziness": "AUTO"
    }
  }
}
    res = es.search(index="index_name", body=query)
    for hit in res['hits']['hits']:
        result.append(hit["_source"]['incidentnumber'])
    print (result)
    return result



count=0
out=[]
for index, row in df.iterrows():
    search_query=str(row['User problem'])
    category=str(row['Category'])
    print(category)
    type_=str(row['Type'])
    print(type_)
    print(search_query)
    response=search(search_query,category,type_)
    print(df['Knowledge ID'][count])
    if (row['Knowledge ID']) in response:
        print(response.index((row['Knowledge ID'])))
        out.append(response.index((row['Knowledge ID'])))
    else:
        out.append("not under top 10")
    count+=1


dataframe = pd.DataFrame({'output':out})

dataframe.to_csv('testES.csv', sep=',')


