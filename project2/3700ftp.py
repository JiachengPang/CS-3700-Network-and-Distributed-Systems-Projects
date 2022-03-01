import sys
import socket
import ssl
import urllib.parse
import os
import codecs

"""
A FTP client program.

This program handles basic FTP jobs:
ls <URL>            print out the content in a directory on the FTP server
mkdir <URL>         creates a directory on the FTP server
rm <URL>            remove a file on the FTP server
rmdir <URL>         remove a directory on the FTP server
cp <ARG1> <ARG2>    copy file <ARG1> to file <ARG2>, if one arg is a local path,
                    the other arg must be a FTP URL
mv <ARG1> <ARG2>    move file <ARG1> to file <ARG2>, if one arg is a local path,
                    the other arg must be a FTP URL

A URL is of the form ftps://[USER[:PASSWORD]@]HOST[:PORT]/PATH
"""
class ftpclient:
    def __init__(self, username, password, hostname, port):
        self.username = username
        self.password = password
        self.hostname = hostname
        self.port = port
        self.data_ip = hostname
        self.data_port = 0
        # control socket
        self.control = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # data socket
        self.data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    """
    Check if the server sends a error reply.

    @param string reply: the server reply 
    """
    def checkReply(self, reply):
        if (reply[0] == "4" or reply[0] == "5" or reply[0] == "6"):
            print(reply)
            raise Exception("FTP connection sent an error.")

    """
    Connect to FTP and configure the connection.
    """
    def connectFTP(self):
        self.control.connect((self.hostname, self.port))
        reply = self.recvControl()
        self.checkReply(reply)
        
        # TLS connection
        msg = "AUTH TLS\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)
        
        context = ssl.create_default_context()
        self.control = context.wrap_socket(self.control, server_hostname=self.hostname)
        
        # provide username
        msg = "USER " + self.username + "\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

        # provide password
        msg = "PASS " + self.password + "\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

        # disable the protection buffer
        msg = "PBSZ 0\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

        # set protection level to private
        msg = "PROT P\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

        # set connection to 8-bit binary data mode
        msg = "TYPE I\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

        # set connection to stream mode
        msg = "MODE S\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

        # set connection to file-oriented mode
        msg = "STRU F\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

    """
    Receive a message from the server through the control socket

    @return string: the message
    """
    def recvControl(self):
        return self.control.recv(8192).decode()

    """
    Receive a message from the server through the data socket
    
    @return string: data
    """
    def recvData(self):
        total = []
        while 1:
            reply = self.data.recv(8192)
            if reply == b'':
                break
            else:
                total.append(reply)
        return b''.join(total)

    """
    Request the server to open a data channel, store the given ip address and port.
    """
    def requestDataChannel(self):
        msg = "PASV\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

        if reply[0] == "2":
            net_str = reply[reply.index("(") + 1 : reply.index(")")]

            net_str_arr = net_str.split(",")
            port1 = net_str_arr[4]
            port2 = net_str_arr[5]
            self.data_port = (int(port1) << 8) + int(port2)
        else:
            raise Exception("Failed to open data channel.")

    """
    Connect to the data channel.
    """
    def openDataChannel(self):
        self.data = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data.connect((self.data_ip, self.data_port))
    
    """
    Unwrap and close the data channel.
    """
    def closeDataChannel(self):
        self.data.unwrap().close()


    """
    Handles the creation and connection of data channel, after the data channel is connected,
    wrap the socket with TLS.

    Then sends the given command to the server through the control channel.

    @param string cmd: the command to be sent to the FTP server
    """
    def handleDataChannel(self, cmd):
        self.requestDataChannel()
        self.control.send(cmd.encode())
        self.openDataChannel()
        reply = self.recvControl()

        if (reply[0] == "4" or reply[0] == "5" or reply[0] == "6"):
            self.closeDataChannel()
            raise Exception("Failed to connect to data channel.")

        context = ssl.create_default_context()
        self.data = context.wrap_socket(self.data, server_hostname=self.data_ip)

    """
    Get the content in a directory on the FTP server.

    @param string path: the directory path
    """
    def list(self, path):
        msg = "LIST " + path + "\r\n"
        self.handleDataChannel(msg)

        data = self.recvData().decode()

        self.closeDataChannel()

        reply = self.recvControl()
        self.checkReply(reply)

        return data

    """
    Download a file from the FTP server.

    @param string path: the file path
    @return string: the content of the file
    """
    def download(self, path):
        msg = "RETR " + path + "\r\n"
        self.handleDataChannel(msg)

        data = self.recvData()

        self.closeDataChannel()

        reply = self.recvControl()
        self.checkReply(reply)

        return data

    """
    Upload a file to the FTP server.

    @param string path: the file path
    @param string content: the content of the file
    """
    def upload(self, path, content):
        msg = "STOR " + path + "\r\n"
        self.handleDataChannel(msg)

        self.data.sendall(content)
        
        self.closeDataChannel()
        
        reply = self.recvControl()
        self.checkReply(reply)

    """
    Remove a file on the server.

    @param string path: the file path
    """
    def remove(self, path):
        msg = "DELE " + path + "\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)

    """
    Create a directory on the server.

    @param string path: the directory path
    """
    def mkdir(self, path):
        msg = "MKD " + path + "\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)
    
    """
    Remove a directory on the server.

    @param string path: the directory path
    """
    def rmdir(self, path):
        msg = "RMD " + path + "\r\n"
        self.control.send(msg.encode())
        reply = self.recvControl()
        self.checkReply(reply)   

    """
    Close the connection with the FTP server (control channel).
    """
    def close(self):
        self.control.send("QUIT\r\n".encode())
        reply = self.recvControl()
        self.checkReply(reply)


"""
The main function.
"""
if __name__ == "__main__":
    url1 = urllib.parse.ParseResult
    url2 = urllib.parse.ParseResult

    # parse url from the command line and check validity
    # there must be 3 or 4 arguments and at least 1 FTP url
    if len(sys.argv) == 3:
        url1 = urllib.parse.urlparse(sys.argv[2])
        if url1.scheme != "ftps":
            raise Exception("Invalid command.")
    elif len(sys.argv) == 4:
        url1 = urllib.parse.urlparse(sys.argv[2])
        url2 = urllib.parse.urlparse(sys.argv[3])
        if url1.scheme != "ftps" and url2.scheme != "ftps":
            raise Exception("Invalid command.")
    else:
        raise Exception("Invalid command.")

    connection = urllib.parse.ParseResult
    if url1.scheme == "ftps":
        connection = url1
    else:
        connection = url2

    # set default port to 21
    port = 21
    if connection.port != None:
        port = connection.port
    
    # set default username to "anonymous" with no password
    username = "anonymous"
    password = ""
    if connection.username != None:
        username = connection.username
        password = connection.password
    
    # create fptclient object and connect to FTP
    client = ftpclient(username, password, connection.hostname, port)
    client.connectFTP()
    
    # do command
    if sys.argv[1] == "mkdir":
        client.mkdir(url1.path)
    elif sys.argv[1] == "rmdir":
        client.rmdir(url1.path)
    elif sys.argv[1] == "ls":
        print(client.list(url1.path))
    elif sys.argv[1] == "rm":
        client.remove(url1.path)    
    elif sys.argv[1] == "cp":
        if url1.path == "" or url2.path == "":
            raise Exception("Invalid command.")
        # upload file if from is local and to is FTP
        if url1.scheme != "ftps":
            f = open(url1.path, 'rb')
            client.upload(url2.path, f.read())
            f.close
        # download file if from is FTP and to is local
        elif url2.scheme != "ftps":
            f = open(url2.path, 'wb')
            f.write(client.download(url1.path))
            f.close()
        # remote copy on FTP if both from and to are FTP
        else:
            data = client.download(url1.path)
            client.upload(url2.path, data)
    elif sys.argv[1] == "mv":
        if url1.path == "" or url2.path == "":
            raise Exception("Invalid command.")

        # move to FTP if from is local and to is FTP
        if url1.scheme != "ftps":
            f = open(url1.path, 'rb')
            client.upload(url2.path, f.read())
            f.close()
            os.remove(url1.path)
        # move to local if from is FTP and to is local
        elif url2.scheme != "ftps":
            f = open(url2.path, 'wb')
            f.write(client.download(url1.path))
            f.close()
            client.remove(url1.path)
        # remote move on FTP is both from and to are FTP
        else:
            data = client.download(url1.path)
            client.upload(url2.path, data)
            client.remove(url1.path)
    else:
        raise Exception("Invalid command.")

    # close connection
    client.close()
