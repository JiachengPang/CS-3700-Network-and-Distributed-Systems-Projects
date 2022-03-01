# CS3700 - Project 2

Author: Jiacheng Pang(001484188)


This project is developed in python. The logic flow is as follows:  

Read command line inputs -> parse connection attributes -> connect to FTP -> execute command -> close connection with FTP  

If a data channel is required during the command execution, the program follows the correct steps communicating with the FTP server through 2 sockets and handles uploading and downloading data from the data socket.    

The program defines a class ftpclient that handles the communication with the FTP server and the main method controls the ftpclient object to perform the specified command.  

If the program receives invalid command or receives an error message from the FTP server, it throws an exception and exits.

Most of this project seemed straightforward. The biggest challenge I encountered was not developing the program itself, but understanding what FTP protocol is and what the correct communication flow with the FTP server should be, both for the control channel and the data channel.  

To upload or download a file, I first had trouble with the data channel as I did not know what type of data the client should send or receive, or whether the file type should affect the data transmission. After some research, I decided that the type of data or the type of the file does not matter as the content of the file should be treated as bytes. I got it to work with some simple code using the file operations in python.  

I tested the program mostly using STDIN debug message and looking at the results on the provided ftps://ftp.3700.network server. I printed out the messages received from the server as the program ran to keep track of the communication with the FTP server. Finally, I tested every expected functionality against the provied FTP server to make sure the program works. 
