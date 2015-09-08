__author__ = 'luisangel'
import tweepy
import time

from User_location import *
from neo4jrestclient.client import GraphDatabase


FOLLOWERS_OF_FOLLOWERS_LIMIT = 500
DEPTH = 3
SEMILLA = "soypuertomontt"
BDJSON = "/home/luisangel/twitter-users"

enc = lambda x: x.encode('ascii', errors='ignore')

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")
CONSUMER_KEY = 'QjBYJl9mapJBHrf3HJTYemHIi'
CONSUMER_SECRET = 'UUaOitJpHZWAS026fbqzjAUWPsY8FJf5VvycCwDjXSqAIpheZY'

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located
# under "Your access token")
ACCESS_TOKEN = '126471512-4aJQ4pBXoE7ADoWqQ3Sms3SvaDCYhQlJEnjqQ2U5'
ACCESS_TOKEN_SECRET = 'ZBvbDXB1BAjh3zWnLZG7AW1QyHvaQAHHTF3iUoMRLdtFX'

# == OAuth Authentication ==
#
# This mode of authentication is the new preferred way
# of authenticating with Twitter.
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)


def getConecction():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb

def createUserJson(user={},userfname=str()):
    with open(userfname, 'w') as outf:
        outf.write(json.dumps(user, indent=1))
        print("UserJson id: "+str(user['id'])+" creado.")

def createUserNode(gdb, user={}):
    u=user.copy()
    n = gdb.node()
    if u.get('followers_ids') is not None:
        del(u['followers_ids'])
    n.properties = u
    n.labels.add("User")
    if n.get('chile') is True:
        n.labels.add("Chile")
        if n.get('screen_name') == SEMILLA:
            n.labels.add("Semilla")
    else:
        n.labels.add("Extranjero")
    return n


def getListUsers(gdb):
    query = "MATCH (n:User) RETURN n LIMIT 25"
    results = gdb.query(query, data_contents=True)
    return results

def getUserNodeById(gdb,id):
    query="MATCH (n:User)WHERE n.id={id} RETURN n LIMIT 25"
    param={'id':id}
    results = gdb.query(query, params=param,data_contents=True)
    if results.rows is not None:
        u=results.rows[0][0]

def getIdUserNodo(gdb,id):
    query="MATCH (n:User) WHERE n.id={id} RETURN {id: ID(n)} as n"
    param={'id':id}
    results = gdb.query(query, params=param,data_contents=True)
    if len(results.rows)>1:
        print("WARNING: ID CON MAS DE UN NODO ASIGNADO.")
    return results.rows[0][0]['id']

def getUser(gdb, id):
    existe=False ## Para evitar nodos repetidos
    ##USAMOS NEO4J
    query="MATCH (n:User)WHERE n.id={id} RETURN n LIMIT 25"
    param={'id':id}
    results = gdb.query(query, params=param,data_contents=True)
    if results.rows is not None:
        existe=True
        u=results.rows[0][0]
        if u['chile'] is False:
            return u
    ##USAMOS JSON
    userfname = os.path.join(BDJSON, str(id) + '.json')

    if os.path.exists(userfname):
        user = json.loads(file(userfname).read())
        if existe is False:
            createUserNode(gdb, user)
        return user
    ##USAMOS APITWITTER

    u = api.get_user(id)

    if u.protected is True:
        return None

    user = {'location': u.location}
    user['id']=id
    user = is_location(user)

    if user['chile'] is True:
        user['screen_name'] = str(u.screen_name)
        user['followers_count'] = int(u.followers_count)
        user['followers_ids'] = u.followers_ids()
        createUserJson(user,userfname)
    if existe is False:
        createUserNode(gdb, user)
    return user

def getRelationById(gdb, id):
    query = "MATCH ()-[role:Follower]->() where role.id={id} return role"
    param = {'id': id}
    results = gdb.query(query, params=param, data_contents=True)
    return results.rows


def createRelation(gdb, node1, node2):
    idd = str(node1.id) + "r" + str(node2.id)
    rels = getRelationById(gdb, idd)
    if rels is None:
        rels = node1.relationships.create("Follower", node2, id=idd)
    return rels


def get_follower_ids(centre, max_depth=1, current_depth=0, taboo_list=[]):
    # print 'current depth: %d, max depth: %d' % (current_depth, max_depth)
    # print 'taboo list: ', ','.join([ str(i) for i in taboo_list ])

    if current_depth == max_depth:
        print 'out of depth'
        return taboo_list

    if centre in taboo_list:
        # we've been here before
        print 'Ya estuvimos en el usuario ID: ' + str(centre)
        return taboo_list
    else:
        taboo_list.append(centre)

    try:
        gdb=getConecction()
        while True:
            try:
                user=getUser(gdb,centre)
                if user is None or user['chile'] is False:
                    return taboo_list
                print 'Recuperacion de datos de usuario de Twitter id %s' % str(centre)
                break
            except tweepy.TweepError, error:
                print type(error)

                if str(error) == 'Not authorized.':
                    print 'No se puede acceder a los datos de usuario - no autorizado.'
                    return taboo_list

                if str(error) == 'User has been suspended.':
                    print 'Usuario suspendido.'
                    return taboo_list

                errorObj = error[0][0]

                print errorObj

                if errorObj['message'] == 'Rate limit exceeded':
                    print 'Rate limited. Dormir durante 15 minutos.'
                    time.sleep(15 * 60 + 15)
                    continue

                return taboo_list

        cd = current_depth
        followerids=user['followers_ids']
        if cd + 1 < max_depth:
            idnodo=getIdUserNodo(gdb,user['id'])
            nodo=gdb.node[idnodo]
            for fid in followerids[:int(FOLLOWERS_OF_FOLLOWERS_LIMIT)]:
                user2=None
                while True:
                    try:
                        user2=getUser(gdb,fid)
                        break
                    except tweepy.TweepError, e:
                        # hit rate limit, sleep for 15 minutes
                        print 'Rate limited. Dormir durante 15 minutos. ' + e.reason
                        time.sleep(15 * 60 + 15)
                        continue
                    except StopIteration:
                        break

                if user2 is None:
                    continue

                idnodo2=getIdUserNodo(gdb,user2['id'])
                nodo2=gdb.node[idnodo2]

                createRelation(gdb,nodo,nodo2)
                taboo_list = get_follower_ids(fid, max_depth=max_depth,
                                                  current_depth=cd + 1, taboo_list=taboo_list)

        if cd + 1 < max_depth and len(followerids) > FOLLOWERS_OF_FOLLOWERS_LIMIT:
                print 'No todos los seguidores fueron recuperados para %d.' % centre

    except Exception, error:
        print 'Error al recuperar los seguidores de usuario id: '+ str(centre)
        print error
        return taboo_list
        sys.exit(1)

    return taboo_list


def main():
    screenname = SEMILLA
    depth = int(DEPTH)

    if depth < 1 or depth > 3:
        print 'Depth value %d is not valid. Valid range is 1-3.' % depth
        sys.exit('Invalid depth argument.')

    print 'Max Depth: %d' % depth

    matches = api.lookup_users(screen_names=[screenname])  # Busca Usuario Twitter

    if len(matches) == 1:
        print get_follower_ids(matches[0].id, max_depth=depth)
    else:
        print 'Lo sentimos, no pudo encontrar el usuario de Twitter con screen name: %s' % screenname



def prueba():

    gdb=getConecction()
    id=2182892282
    u={'id':2182892282,'location':'Tarapaca'}

    print is_location(u)


if __name__ == "__main__":
    main()





