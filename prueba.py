__author__ = 'luisangel'

from neo4jrestclient.client import GraphDatabase
import os
import json
import unicodedata

BDJSON = "/home/luisangel/twitter-users"
enc = lambda x: x.encode('ascii', errors='ignore')
courses = {
    'feb2012': {'cs101': {'name': 'Building a Search Engine',
                          'teacher': 'Dave',
                          'assistant': 'Peter C.'},
                'cs373': {'name': 'Programming a Robotic Car',
                          'teacher': 'Sebastian',
                          'assistant': 'Andy'}},
    'apr2012': {'cs101': {'name': 'Building a Search Engine',
                          'teacher': 'Dave',
                          'assistant': 'Sarah'},
                'cs212': {'name': 'The Design of Computer Programs',
                          'teacher': 'Peter N.',
                          'assistant': 'Andy',
                          'prereq': 'cs101'},
                'cs253':
                    {'name': 'Web Application Engineering - Building a Blog',
                     'teacher': 'Steve',
                     'prereq': 'cs101'},
                'cs262':
                    {'name': 'Programming Languages - Building a Web Browser',
                     'teacher': 'Wes',
                     'assistant': 'Peter C.',
                     'prereq': 'cs101'},
                'cs373': {'name': 'Programming a Robotic Car',
                          'teacher': 'Sebastian'},
                'cs387': {'name': 'Applied Cryptography',
                          'teacher': 'Dave'}},
    'jan2044': {'cs001': {'name': 'Building a Quantum Holodeck',
                          'teacher': 'Dorina'},
                'cs003': {'name': 'Programming a Robotic Robotics Teacher',
                          'teacher': 'Jasper'},
                }
}


def getConecction():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb


def createUser(gdb, user={}):
    n = gdb.node()
    n.properties = user
    n.labels.add("User")
    return n


def getListUsers(gdb):
    query = "MATCH (n:User) RETURN n LIMIT 25"
    results = gdb.query(query, data_contents=True)
    return results


def getUser(gdb, id):
    query = "MATCH (n:User) WHERE n.id={id} RETURN n LIMIT 25"
    param = {'id': id}
    results = gdb.query(query, params=param, data_contents=True)

    return results.rows


def getRelationById(gdb, id):
    query = "MATCH ()-[role:Knows]->() where role.id={id} return role"
    param = {'id': id}
    results = gdb.query(query, params=param, data_contents=True)

    return results


def getIdUserNodo(gdb, id):
    query = "MATCH (n:User) WHERE n.id={id} RETURN {id: ID(n)} as n"
    param = {'id': id}
    results = gdb.query(query, params=param, data_contents=True)
    if len(results.rows) > 1:
        print("WARNING: ID CON MAS DE UN NODO ASIGNADO.")
    return results.rows[0][0]['id']


def involved(courses):
    for time1 in courses:
        for course in courses[time1]:
            for info in time1[course]:
                print info


def main():
    try:
        involved(courses)
    except Exception, ex:
        e1 = "string indices must be integers not str"
        e2 = "string indices must be integers, not str"
        if ex.message == e1 or ex.message == e2:
            print("Siii")
        print("El error es: " + ex.message)


if __name__ == "__main__":
    main()
