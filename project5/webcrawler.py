import sys
import socket
from html.parser import HTMLParser
import ssl
import re
from collections import deque

RECV_SIZE = 1024

class Crawler:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.login = False
        self.port = 443
        self.hostname =  'fakebook.3700.network'
        self.sessionID = ''
        self.cookie = ''
        self.socket = None
        self.sent = []
        self.willSend = []
        self.secretFlags = []

    def initServer(self):
        print("init")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        context = ssl.create_default_context()
        sock = socket.create_connection((self.hostname, self.port))
        self.socket = context.wrap_socket(sock, server_hostname=self.hostname)

    def loginServer(self):
        print("login")
        response = self.sendGetRequest('/accounts/login/')
        self.setCookie(response)
        request = self.LoginMessage()
        self.socket.sendall(request.encode())
        response = self.socket.recv(RECV_SIZE).decode()
        self.setCookie(response)

    def crawl(self):
        print("begin crawling")
        self.willSend +=['/fakebook/']

        while len(self.secretFlags) < 5:
            if len(self.willSend) == 0:
                self.willSend +=['/fakebook/']
            try:
                path = self.willSend[0]
                del self.willSend[0]
                if path in self.sent:
                    print(path + "already visited")
                    continue
                response = self.sendGetRequest(path)
                self.setCookie(response)
                status = self.returnStatus(response)
                self.sent += [path]
                print(path)
                if status == 200:
                    # print(200)
                    self.findURLs(response)
                    self.findSecretFlag(response)
                elif status == 302:
                    print("302")
                    self.redirect(response)
                elif status == 403 or status == 404:
                    print("402")
                elif status == 500:
                    print("500")
                    continue
            except ssl.SSLZeroReturnError:
                self.initServer()
                self.loginServer()
        print(self.secretFlags)

    def sendGetRequest(self, path):
        request = "GET " + path + " HTTP/1.1\r\nHost: " + self.hostname
        request += "\r\nCookie: csrftoken=" + self.cookie + "; sessionid="
        request += self.sessionID + ";\r\nConnection:keep-alive\r\n\r\n"
        self.socket.sendall(request.encode())
        response = self.socket.recv(RECV_SIZE).decode()
        if '<!DOCTYPE html' in response:
            while '</html>' not in response:
                response += self.socket.recv(RECV_SIZE).decode()
        return response

    def setCookie(self, response):
        if 'csrftoken=' in response:
            temp = response.split('csrftoken=')
            temp = temp[1].split(';')
            csrftoken = temp[0]
            self.cookie = csrftoken
        if 'sessionid=' in response:
            temp2 = response.split('sessionid=')
            temp2 = temp2[1].split(';')
            sessionID = temp2[0]
            if sessionID == '""':
                self.sessionID = ''
            else:
                self.sessionID = sessionID

    def LoginMessage(self):
        content = "csrfmiddlewaretoken=" + self.cookie + "&username="
        content += self.username + "&password=" + self.password
        content += "&next=%2Ffakebook%2F"
        request = 'POST /accounts/login/?next=/fakebook/ HTTP/1.1\r\nHost: ' + self.hostname
        request += '\r\nReferrer: ' + self.hostname + '/accounts/login/\r\n'
        request += 'Cookie: csrftoken=' + self.cookie + '; sessionid='
        request += self.sessionID + '\r\nContent-Type: '
        request += 'application/x-www-form-urlencoded\r\nContent-Length: '
        request += str(len(content)) + "\r\nConnection:keep-alive\r\n\r\n"
        request += content + "\r\n"
        return request

    def findURLs(self, response):
        urls = set(re.findall('href=[\'"]?(/fakebook/[^\'" >]+)', response))
        urls = list(urls)
        for url in urls:
            if url in self.sent:
                continue
            else:
                self.willSend = [url] + self.willSend

    def findSecretFlag(self, response):
        if "FLAG" in response:
            flags = re.findall('FLAG: ([^\'\" ><]+)', response)
            for flag in flags:
                self.secretFlags += [flag]
                print(flag)

    def redirect(self, response):
        print("redirect")
        response = response.split('Location: ')
        response = response[1].split('\n')
        redirectPath = response[0].split('\r')[0]
        self.willSend = [redirectPath] + self.willSend

    def returnStatus(self, response):
        if '200 OK' in response:
            return 200
        elif '302 Found' in response:
            return 302
        elif '403 Forbiden' in response:
            return 403
        elif '404 Not Found' in response:
            return 404
        elif '500 Internal Server Error' in response:
            return 500


if __name__ == '__main__':
    print("starting")
    username = sys.argv[1]
    password = sys.argv[2]
    crawler = Crawler(username, password)
    crawler.initServer()
    crawler.loginServer()
    crawler.crawl()
