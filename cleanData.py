__author__ = 'luisangel'
# Crea el atributo Chile en cada USuario si no lo tiene.
import glob
from collections import defaultdict
from User_location import *

BDJSON = "/home/luisangel/twitter-users/"
users = defaultdict(lambda: { 'followers': 0 })

def main():
    mod=0
    for f in glob.glob(BDJSON+'*.json'):
        data = json.load(file(f))
        if data.get('chile') is None:
            data = is_location(data)
            with open(BDJSON+str(data['id'])+'.json', 'w') as outf:
                outf.write(json.dumps(data, indent=1))
            mod=mod+1
            print 'Modificados: %d' % mod


if __name__ == "__main__":
    main()