import click
import base64
import requests
import random
import string
import base64
import hashlib
from http.server import SimpleHTTPRequestHandler
import urllib.parse
import socketserver

from http import HTTPStatus

# EXPERIMENTAL

@click.command()
@click.option("--frontend", default="http://127.0.0.1:8000/")
@click.option("--local-port", default="8010", type=int)
@click.argument("credentials")
def run(credentials, frontend, local_port):
    # clientid:clientsecret
    # 1234567u8i9:0987654321
    ID, SECRET = credentials.split(":")

    credential = "{0}:{1}".format(ID, SECRET)
    cred = base64.b64encode(credential.encode("utf-8")).decode("ascii")
    session = requests.Session()
    session.auth = (ID, SECRET)
    code_verifier = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(random.randint(43, 128)))

    code_challenge = hashlib.sha256(code_verifier.encode('utf-8')).digest()
    code_challenge = base64.urlsafe_b64encode(code_challenge).decode('utf-8').replace('=', '')
    print(f"{frontend}o/authorize/?response_type=code&code_challenge={code_challenge}&code_challenge_method=S256&client_id={ID}&redirect_uri=http://127.0.0.1:{local_port}/callback")

    class Handler(SimpleHTTPRequestHandler):
        code = {}
        def do_GET(self):
            data = urllib.parse.parse_qs(self.path)
            print(data)
            self.code["result"] = data['/callback?code'][0]
            print(data)

            self.send_response(HTTPStatus.OK)
            self.end_headers()
            self.wfile.write(b"Successfully set callback code")

    with socketserver.TCPServer(("", int(local_port)), Handler) as httpd:
        print("serving at port", local_port)
        httpd.handle_request()
    print(Handler.code)
    res = session.post(f"{frontend}o/token/", data={"grant_type": "authorization_code", "client_id": ID, "client_secret": SECRET, "code": Handler.code["result"], "code_verifier": code_verifier}, headers={
    "Content-Type": "application/x-www-form-urlencoded"
    })
    print(res)
    print(res.json())
    print(f"Authorization: Bearer {res.json()['access_token']}")

if __name__ == "__main__":
    run()
