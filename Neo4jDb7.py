__author__ = 'luisangel'
import tweepy
import time
import MySQLdb
import Credentials as k
from User_location import *
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import exceptions

FOLLOWERS_OF_FOLLOWERS_LIMIT = 3000000
DEPTH = 2
SEMILLA = "Alexis_Sanchez"#"soyquillota"
BDJSON = "../../twitter-users"
ID_BAD = 0
enc = lambda x: x.encode('ascii', errors='ignore')

# The consumer keys can be found on your application's Details
# page located at https://dev.twitter.com/apps (under "OAuth settings")
CONSUMER_KEY = 'HFvyfF6HwRmiTmPUzyDfEEQjN'
CONSUMER_SECRET = 'm2NBerSxyry71jSciVxxFBT0gD7qyVo8xHK4HIhWHQYZGWz4ey'

# The access tokens can be found on your applications's Details
# page located at https://dev.twitter.com/apps (located
# under "Your access token")
ACCESS_TOKEN = '126471512-rUxowItDqWG4JJ4QykaIIU5nZxNhlev7X1J9AfOd'
ACCESS_TOKEN_SECRET = 'MCrfnd7O4Hebrl1eFbNKg8u7Wsu3AHdutoUnEI0pXh84D'

# == OAuth Authentication ==
#
# This mode of authentication is the new preferred way
# of authenticating with Twitter.
auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = tweepy.API(auth)


def get_connection_sql():
    # Returns a connection object whom will be given to any DB Query function.

    try:
        connection = MySQLdb.connect(host=k.GEODB_HOST, port=3306, user=k.GEODB_USER,
                                     passwd=k.GEODB_KEY, db=k.GEODB_NAME)
        return connection
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def insert_lost_user(connection, id_user):

    try:
        x = connection.cursor()
        x.execute('INSERT INTO LostUser VALUES (%s) ', (
            id_user,))
        connection.commit()
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        connection.rollback()
    pass


def get_id_lost(connection):
    query = "SELECT idLostUser FROM LostUser ;"
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        if data is None:
            return None
        else:
            return [x[0] for x in data]
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1


def close_connection_sql(connection):
    connection.close()


def getConecction():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb

def createUserJson(user={},userfname=str()):
    with open(userfname, 'w') as outf:
        outf.write(json.dumps(user, indent=1))
        print("UserJson id: "+str(user['id'])+" creado.")


def getIdsBySemilla(gdb, semilla):  # Ids por semilla
    query = "MATCH (n:Chile)-->(p) WHERE n.screen_name={sn} RETURN collect(p.id) as n"
    param = {'sn': semilla}
    results = gdb.query(query, params=param, data_contents=True)
    if len(results.rows) > 1:
        print "WARNING: ID CON MAS DE UN NODO ASIGNADO. Id: " + str(id)
    return results.rows[0][0]



def createUserNode(gdb, user={}):
    try:
        u = user.copy()
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
    except exceptions.StatusException as e:
        n.delete()
        print "Ocurrio el siguiente error: "+e.result
        return None

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
        print "WARNING: ID CON MAS DE UN NODO ASIGNADO. Id: " + str(id)
    return results.rows[0][0]['id']

def getUser(gdb, id):
    existe = False ## Para evitar nodos repetidos
    ##USAMOS NEO4J
    query="MATCH (n:User)WHERE n.id={id} RETURN n LIMIT 25"
    param={'id':id}
    results = gdb.query(query, params=param, data_contents=True)
    if results.rows is not None:
        existe = True
        u=results.rows[0][0]
        if u['chile'] is False:
            return u
    ##USAMOS JSON
    userfname = os.path.join(BDJSON, str(id) + '.json')

    if os.path.exists(userfname):
        user = json.loads(file(userfname).read())
        if existe is False:
            if createUserNode(gdb, user) is None:
                return None
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
        if createUserNode(gdb, user) is None:
            return None
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
                print 'user id %s' % str(centre)
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
        cn = get_connection_sql()
        id_loser = get_id_lost(cn)
        close_connection_sql(cn)

        set_id_loser = set(id_loser)
        setSemilla = set(getIdsBySemilla(gdb, str(user['screen_name'])))
        setFollowerId = set(user['followers_ids'])


        followerids = list(setFollowerId - setSemilla-set_id_loser)
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
                        print "Primero: "+e.reason+" Termina."
                        if e.reason == 'Failed to send request: (\'Connection aborted.\', gaierror(-2, \'Name or service not known\'))':
                            print 'Internet. Dormir durante 1 minuto. ' + e.message
                            time.sleep(60)
                            continue
                        if e.reason == 'Failed to send request: HTTPSConnectionPool(host=\'api.twitter.com\', port=443): Read timed out. (read timeout=60)':
                            print 'Internet. Dormir durante 1 minuto. ' + e.message
                            time.sleep(60)
                            continue
                        if e.message[0]['code'] == 34:
                            print "Not found ApiTwitter id: "+str(centre)+" fid= "+str(fid)
                            cn = get_connection_sql()
                            insert_lost_user(cn, fid)
                            close_connection_sql(cn)
                            break
                        if e.message[0]['code'] == 63:
                            print 'Usuario suspendido:'+str(centre)+" fid= "+str(fid)
                            cn = get_connection_sql()
                            insert_lost_user(cn, fid)
                            close_connection_sql(cn)
                            break
                        else:
                            global ID_BAD
                            if ID_BAD == fid:
                                print "Id: %d durmio dos veces." % fid
                                break
                            ID_BAD = fid
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
        print 'Error al recuperar los seguidores de usuario id: '+ str(centre)+'y con fid = '+str(fid)
        print error
        return taboo_list
        sys.exit(1)

    return taboo_list


def getIds(sn):
    ides = []
    i = 0
    for page in tweepy.Cursor(api.followers_ids, screen_name=sn).pages():
        ides.extend(page)
        i += 1
        print "Avance: %d" % i
        time.sleep(60)
    return ides


def main():
    screenname = SEMILLA
    depth = int(DEPTH)

    if depth < 1 or depth > 3:
        print 'Depth value %d is not valid. Valid range is 1-3.' % depth
        sys.exit('Invalid depth argument.')

    print 'Max Depth: %d' % depth

    matches = api.lookup_users(screen_names=[screenname])  # Busca Usuario Twitter

    if len(matches) == 1:
        print len(get_follower_ids(matches[0].id, max_depth=depth))
    else:
        print 'Lo sentimos, no pudo encontrar el usuario de Twitter con screen name: %s' % screenname



def prueba():

    gdb=getConecction()
    id=2182892282
    u={'id':2182892282,'location':'Tarapaca'}

    print is_location(u)


if __name__ == "__main__":
    main()