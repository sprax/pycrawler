
import time
import socket
import multiprocessing
import multiprocessing.connection

XmlListener = multiprocessing.connection.XmlListener
Listener = multiprocessing.connection.Listener

def Client(address, family=None, authkey=None, timeout=None):
    family = family or address_type(address)
    if family == 'AF_PIPE':
        c = PipeClient(address, timeout=timeout)
    else:
        c = SocketClient(address, timeout=timeout)

    if authkey is not None and not isinstance(authkey, bytes):
        raise TypeError, 'authkey should be a byte string'

    if authkey is not None:
        answer_challenge(c, authkey)
        deliver_challenge(c, authkey)

def PipeClient(address, timeout=None):
    raise Exception, "PipeClient not implemented."

def SocketClient(address, timeout=None):
    '''
    Return a connection object connected to the socket given by `address`
    '''
    family = address_type(address)
    s = socket.socket( getattr(socket, family) )

    if timeout:
        end_time = time.time() + timeout

    while 1:
        try:
            if timeout:
                cur_timeout = end_time - time.time()
                if cur_timeout < 0:
                    raise socket.timeout, "timed out."
                s.settimeout(cur_timeout)
            s.connect(address)
        except socket.error, e:
            if e.args[0] != errno.ECONNREFUSED: # connection refused
                debug('failed to connect to address %s', address)
                raise
            time.sleep(0.01)
        else:
            break
    else:
        raise

    fd = multiprocessing.connection.duplicate(s.fileno())
    conn = multiprocessing.connection._multiprocessing.Connection(fd)
    s.close()
    return conn

def XmlClient(*args, **kwds):
    global xmlrpclib
    import xmlrpclib
    return multiprocessing.connection.ConnectionWrapper(Client(*args, **kwds),
                                                        _xml_dumps,
                                                        _xml_loads)
