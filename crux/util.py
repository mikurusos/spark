import cPickle as pickle


def loadPickle(path):
    try:
        print "loading..."
        with open(path, "rb") as f:
            gender = pickle.load(f)
        print "finished!"
    except:
        gender = {}
    finally:
        return gender