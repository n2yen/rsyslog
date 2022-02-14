# call this via "python[3] script name"
import argparse
import json
import os
import zlib
import base64
import time

try:
    from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer # Python 2
except ImportError:
    from http.server import BaseHTTPRequestHandler, HTTPServer # Python 3

# Keep track of data received at each path
data = {}
data_compress = {}
metadata = {'posts': 0, 'fail_after': 0, 'fail_every': -1, 'decompress': False, 'userpwd': ''}
Debug = 0

class MyHandler(BaseHTTPRequestHandler):
    """
    POST'd data is kept in the data global dict.
    Keys are the path, values are the raw received data.
    Two post requests to <host>:<port>/post/endpoint means data looks like...
        {"/post/endpoint": ["{\"msgnum\":\"00001\"}", "{\"msgnum\":\"00001\"}"]}

    GET requests return all data posted to that endpoint as a json list.
    Note that rsyslog usually sends escaped json data, so some parsing may be needed.
    A get request for <host>:<post>/post/endpoint responds with...
        ["{\"msgnum\":\"00001\"}", "{\"msgnum\":\"00001\"}"]
    """

    def validate_auth(self):
        # header format for basic authentication
        # 'Authorization: Basic <base 64 encoded uid:pwd>'
        if 'Authorization' not in self.headers:
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b'missing "Authorization" header')
            return False

        auth_header = self.headers['Authorization']
        _, b64userpwd = auth_header.split()
        userpwd = base64.b64decode(b64userpwd).decode('utf-8')
        if userpwd != metadata['userpwd']:
            self.send_response(401)
            self.end_headers()
            self.wfile.write(b'invalid auth: {0}'.format(userpwd))
            return False

        return True

    def do_POST(self):
        metadata['posts'] += 1

        if metadata['userpwd']:
            if not self.validate_auth():
                return
        response_delay_secs = metadata['fail_with_delay_secs']

        if metadata['delay_response_at'] != 0 and metadata['posts'] == metadata['delay_response_at']:
            print("request '{0}' is being delayed for: {1} secs".format(metadata['posts'], metadata['delay_secs']))
            #print "request '{0}' is being delayed for: {1} secs".format(metadata['posts'], metadata['delay_secs']))
            time.sleep(metadata['delay_secs'])

        if metadata['fail_at'] != -1 and metadata['posts'] > metadata['fail_at']:
            if response_delay_secs:
                print("sleeping for response_delay_secs: {1}".format(response_delay_secs))
                time.sleep(response_delay_secs)
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'BAD REQUEST')
            return

        if metadata['fail_with_400_after'] != -1 and metadata['posts'] > metadata['fail_with_400_after']:
            if response_delay_secs:
                print("sleeping for response_delay_secs: {0}".format(response_delay_secs))
                time.sleep(response_delay_secs)
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b'BAD REQUEST')
            return

        if metadata['posts'] > 1 and metadata['fail_every'] != -1 and metadata['posts'] % metadata['fail_every'] == 0:
            if response_delay_secs:
                print("sleeping for response_delay_secs: {0}".format(response_delay_secs))
                time.sleep(response_delay_secs)
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'INTERNAL ERROR')
            return

        content_length = int(self.headers['Content-Length'] or 0)
        raw_data = self.rfile.read(content_length)

        if self.path not in data_compress:
            data_compress[self.path] = []
        data_compress[self.path].append(raw_data)

        if metadata['decompress']:
            post_data = zlib.decompress(raw_data, 31)
        else:
            post_data = raw_data

        #self.log_message("omhttp - received post_data: '{0}'".format(post_data))
        if self.path not in data:
            data[self.path] = []
        data[self.path].append(post_data.decode('utf-8'))

        res = json.dumps({'msg': 'ok'}).encode('utf8')

        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(res))
        self.end_headers()

        self.wfile.write(res)
        return

    def do_GET(self):
        # print out contents of data_compress
        if Debug:
            self.log_message("do get called, printing out data_decompress...")
            for elm in data_compress[self.path]:
                raw_data = zlib.decompress(elm, 31)
                #data = raw_data.decode('utf-8')
                self.log_message(raw_data.decode('utf-8'))
            self.log_message("done.")

        if self.path in data:
            result = data[self.path]
        else:
            result = []

        res = json.dumps(result).encode('utf8')

        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(res))
        self.end_headers()

        self.wfile.write(res)
        return


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Archive and delete core app log files')
    parser.add_argument('-p', '--port', action='store', type=int, default=8080, help='port')
    parser.add_argument('-i', '--interface', action='store', type=str, default='localhost', help='port')
    parser.add_argument('--fail-after', action='store', type=int, default=0, help='start failing after n posts')
    parser.add_argument('--fail-every', action='store', type=int, default=-1, help='fail every n posts')
    parser.add_argument('--fail-at', action='store', type=int, default=-1, help='fail at n posts')
    parser.add_argument('--fail-with-400-after', action='store', type=int, default=-1, help='fail with 400 after n posts')
    parser.add_argument('--fail-with-delay-secs', action='store', type=int, default=0, help='fail with n secs of delay')
    parser.add_argument('--delay-response-at', action='store', type=int, default=0, help='delay response at n posts')
    parser.add_argument('--delay-secs', action='store', type=int, default=2, help='number of secs during delay')
    parser.add_argument('--decompress', action='store_true', default=False, help='decompress posted data')
    parser.add_argument('--userpwd', action='store', default='', help='only accept this user:password combination')
    args = parser.parse_args()
    metadata['fail_after'] = args.fail_after
    metadata['fail_every'] = args.fail_every
    metadata['fail_at'] = args.fail_at
    metadata['fail_with_400_after'] = args.fail_with_400_after
    metadata['fail_with_delay_secs'] = args.fail_with_delay_secs
    metadata['delay_response_at'] = args.delay_response_at
    metadata['delay_secs'] = args.delay_secs
    metadata['decompress'] = args.decompress
    metadata['userpwd'] = args.userpwd
    server = HTTPServer((args.interface, args.port), MyHandler)
    pid = os.getpid()
    print('starting omhttp test server at {interface}:{port} with pid {pid}'
          .format(interface=args.interface, port=args.port, pid=pid))
    server.serve_forever()
