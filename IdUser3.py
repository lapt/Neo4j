__author__ = 'luisangel'
import tweepy
import time

from User_location import *
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import exceptions

FOLLOWERS_OF_FOLLOWERS_LIMIT = 3000000
DEPTH = 2
#SEMILLA = "soyarica"
BDJSON = "../../twitter-users"

enc = lambda x: x.encode('ascii', errors='ignore')

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")
CONSUMER_KEY = '7wlc3nNp5Hnmi3adSQYI39fot'
CONSUMER_SECRET = 'q2Ia3unh8MNaAMPTcMPWYSHCLsqfWHd4mOgBP9BvuEhFt8860p'

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located
# under "Your access token")
ACCESS_TOKEN = '126471512-01YsPw2OXopcYw1H8clZUwzGWzrrE4l3gIt4QqWj'
ACCESS_TOKEN_SECRET = 'MzNd5TZxzxKPwpUlZ1ldbPfr8su75hCxt359sCMVzK0SO'

# == OAuth Authentication ==
#
# This mode of authentication is the new preferred way
# of authenticating with Twitter.
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)

SEMILLA2 = ['GermanGarmendia', 'hoytschile', 'TVN', 'T13', 'latercera',
           'CNNChile', 'biobio', 'Cooperativa', 'tv_mauricio', 'Alexis_Sanchez',
          'sebastianpinera', 'StefanKramerS']

SEMILLA = ['soyiquique', 'soycalama', 'soyantofagasta','soycopiapo',
           'soyvalparaiso', 'soyquillota', 'soysanantonio', 'soychillan', 'soysancarlos',
          'soytome','soytalcahuano','soyconcepcion', 'soycoronel', 'soyarauco', 'soytemuco',
           'soyvaldivia', 'soyosorno', 'soypuertomontt', 'soychiloe', 'soychilecl']

SEMILLA3 = ['Emol']

def getConecction():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb

def getIdUserNeo(gdb,sn):
    query="MATCH (n:Chile) WHERE n.screen_name={sn} RETURN {id: n.id} as n"
    param={'sn':sn}
    results = gdb.query(query, params=param,data_contents=True)
    if len(results.rows)>1:
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

def createUserJson(user={},rutaJson=str()):
    with open(rutaJson, 'w') as outf:
        outf.write(json.dumps(user, indent=1))
        print("UserJson id: "+str(user['id'])+" creado.")

def getUserById(rutaJson):
    if os.path.exists(rutaJson):
        user = json.loads(file(rutaJson).read())
        return user
    return None

def main():
    db = getConecction()
    semi = SEMILLA3[0]
    id = getIdUserNeo(db, semi)
    ruta = os.path.join(BDJSON, str(id) + '.json')
    user = getUserById(ruta)
    ids = getIds(semi)
    user['followers_ids'] = ids
    createUserJson(user, ruta)
    print "Nro. Usuarios: %d" % len(ids)



def prueba():
    db=getConecction()
    print getIdUserNeo(db,"soyarica")


if __name__ == '__main__':
    main()


