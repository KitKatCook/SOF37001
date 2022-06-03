import http.server
import socketserver

PORT = 8080
Handler = http.server.SimpleHTTPRequestHandler

class HTTPServer:
    def __init__(self, args = None):
        self.args = args
        with socketserver.TCPServer(("", PORT), Handler) as httpd:
            print("serving at port", PORT)
            httpd.serve_forever()



