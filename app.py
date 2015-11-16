import os
import sys
import logging

import tornado.ioloop
import tornado.web
import tornado.gen
import tornado.websocket
import tornado.httpserver
from tornado.concurrent import Future
import rethinkdb as r
from rethinkdb.errors import RqlRuntimeError, RqlDriverError, ReqlOpFailedError


subscribers = set()
DB = 'data'
TABLE = 'collected'
RETHINKDB_SERVICE_HOST = os.getenv('RETHINKDB_SERVICE_HOST', 'localhost')



@tornado.gen.coroutine
def send_collected_alert():
    while True:
        try:
            temp_conn = yield r.connect(RETHINKDB_SERVICE_HOST, 28015)
            feed = yield r.db(DB).table(TABLE).changes().run(temp_conn)

            while (yield feed.fetch_next()):
                alert = yield feed.next()
                for subscriber in subscribers:
                    subscriber.write_message(alert)
        except Exception as err:
            logging.exception(err)


class WebSocketHandler(tornado.websocket.WebSocketHandler):

    def check_origin(self, origin):
        return True

    def open(self):
        self.stream.set_nodelay(True)
        subscribers.add(self)

    def on_close(self):
        if self in subscribers:
            subscribers.remove(self)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    conn = r.connect(RETHINKDB_SERVICE_HOST, 28015).repl()
    try:
        r.db_create(DB).run()
    except ReqlOpFailedError:
        logging.info('Database already exists')
        try:
            r.db(DB).table_create(TABLE).run()
        except ReqlOpFailedError:
            logging.info('Table already exists')

    conn.close()
    r.set_loop_type("tornado")

    tornado_app = tornado.web.Application([(r'/', WebSocketHandler)])
    server = tornado.httpserver.HTTPServer(tornado_app)
    server.listen(8999)
    tornado.ioloop.IOLoop.current().add_callback(send_collected_alert)
    tornado.ioloop.IOLoop.instance().start()
