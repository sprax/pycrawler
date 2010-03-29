
import multiprocessing
import multiprocessing.managers
import connection

listener_client = {
    'pickle': (connection.Listener, connection.Client),
    'xmlrpclib': (connection.XmlListener, connection.XmlClient),
    }

class BaseManager(multiprocessing.BaseManager):
    listener_client = listener_client

    def __init__(self, address=None, authkey=None, serializer='pickle', timeout=None):
        super(BaseManager, self).__init__(address=address, authkey=authkey,
                                          serializer=serializer)
        self._timeout = timeout

        # Unfortunately, multiprocessing uses a global listener_client,
        # rather than a class object, so it cannot be overridden in children.
        self._Listener, self._Client = self.listener_client[serializer]

    def connect(self, timeout=None):
        """
        Connect manager object to the server process.
        """

        # Unfortunately, multiprocessing.BaseManager.client doesn't use
        # the instance's _Client or _Listener, so we have to override the entire
        # method and replace it.

        if timeout is None:
            timeout = self.timeout

        conn = self._Client(self._address, authkey=self._authkey, timeout=timeout)
        multiprocessing.managers.dispatch(conn, None, 'dummy')
        self._state.value = multiprocessing.managers.State.STARTED
