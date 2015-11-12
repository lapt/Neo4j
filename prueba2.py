__author__ = 'luisangel'
import tweepy
import time
from User_location import *
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import exceptions

FOLLOWERS_OF_FOLLOWERS_LIMIT = 3000000
DEPTH = 2
SEMILLA = "soyarica"
BDJSON = "/home/luisangel/twitter-users"

enc = lambda x: x.encode('ascii', errors='ignore')

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")
CONSUMER_KEY = 'pmsHi3fTyJrqQ6Ov0ANudX2jd'
CONSUMER_SECRET = 'r1sIVRwLumaCJZenTrrQK4HdHaLn749dBeBfl4zx61DsvE3ySH'

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located
# under "Your access token")
ACCESS_TOKEN = '126471512-ld4Cn3Btt4Jbt3hJjXpJep7u9rDFzt8nX6MtxcMa'
ACCESS_TOKEN_SECRET = 'jVDKc4ZBpCfvkGSt7CO44s1mKzfUu4c96E8QNRm0Br80w'

# == OAuth Authentication ==
#
# This mode of authentication is the new preferred way
# of authenticating with Twitter.
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

SEMILLA2 = ['GermanGarmendia', '24HorasTVN', 'TVN', 'T13', 'latercera',
            'CNNChile', 'biobio', 'Cooperativa', 'tv_mauricio', 'SoledadOnetto',
            'sebastianpinera', 'rubionatural']

SEMILLA = ['soyarica', 'soyiquique', 'soycalama', 'soyantofagasta', 'soycopiapo',
           'soyvalparaiso', 'soyquillota', 'soysanantonio', 'soychillan', 'soysancarlos',
           'soytome', 'soytalcahuano', 'soyconcepcion', 'soycoronel', 'soyarauco', 'soytemuco',
           'soyvaldivia', 'soyosorno', 'soypuertomontt', 'soychiloe', 'soychilecl']

SEMILLA3 = ['C1audioBravo']


def getConecction():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb


def getIdUserNeo(gdb, sn):
    query = "MATCH (n:Chile) WHERE n.screen_name={sn} RETURN {id: n.id} as n"
    param = {'sn': sn}
    results = gdb.query(query, params=param, data_contents=True)
    if len(results.rows) > 1:
        print "WARNING: ID CON MAS DE UN NODO ASIGNADO. Id: " + str(id)
    return results.rows[0][0]['id']


def getIds(sn):
    ides = []
    i = 0
    for page in tweepy.Cursor(api.followers_ids, screen_name=sn).pages():
        ides.extend(page)
        i += 1
        print "Avance: %d" % i
        time.sleep(60)
    return ides


def createUserJson(user={}, rutaJson=str()):
    with open(rutaJson, 'w') as outf:
        outf.write(json.dumps(user, indent=1))
        print("UserJson id: " + str(user['id']) + " creado.")


def getUserById(rutaJson):
    if os.path.exists(rutaJson):
        user = json.loads(file(rutaJson).read())
        return user
    return None


def getIdsBySemilla(gdb, semilla):  # Ids por semilla
    query = "MATCH (n:Chile)-->(p) WHERE n.screen_name={sn} RETURN collect(p.id) as n"
    param = {'sn': semilla}
    results = gdb.query(query, params=param, data_contents=True)
    if len(results.rows) > 1:
        print "WARNING: ID CON MAS DE UN NODO ASIGNADO. Id: " + str(id)
    return results.rows[0][0]


def main():
    db = getConecction()
    for semi in SEMILLA3:
        print semi
        id = getIdUserNeo(db, semi)
        ruta = os.path.join(BDJSON, str(id) + '.json')
        user = getUserById(ruta)
        user['followers_ids'] = getIds(semi)
        createUserJson(user, ruta)
    pass


def prueba():
    db = getConecction()
    c = getIdsBySemilla(db, "soytemuco")
    sc = set(c)
    print len(sc)
    print len(c)


if __name__ == '__main__':
    main()
