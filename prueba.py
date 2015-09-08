__author__ = 'luisangel'


from neo4jrestclient.client import GraphDatabase
import os
import json
import unicodedata

BDJSON = "/home/luisangel/twitter-users"
enc = lambda x: x.encode('ascii', errors='ignore')

def getConecction():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb

def createUser(gdb,user={}):
    n=gdb.node()
    n.properties = user
    n.labels.add("User")
    return n
def getListUsers(gdb):
    query="MATCH (n:User) RETURN n LIMIT 25"
    results = gdb.query(query,data_contents=True)
    return results

def getUser(gdb,id):
    query="MATCH (n:User) WHERE n.id={id} RETURN n LIMIT 25"
    param={'id':id}
    results = gdb.query(query, params=param,data_contents=True)

    return  results.rows

def getRelationById(gdb,id):
    query="MATCH ()-[role:Knows]->() where role.id={id} return role"
    param={'id':id}
    results = gdb.query(query, params=param,data_contents=True)

    return results

def getIdUserNodo(gdb,id):
    query="MATCH (n:User) WHERE n.id={id} RETURN {id: ID(n)} as n"
    param={'id':id}
    results = gdb.query(query, params=param,data_contents=True)
    if len(results.rows)>1:
        print("WARNING: ID CON MAS DE UN NODO ASIGNADO.")
    return results.rows[0][0]['id']

def main():
    gdb=getConecction()
    id=2182892282


    print getUser(gdb,id)



if __name__=="__main__":
    main()