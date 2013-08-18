INCS=-I/usr/local/include/libnetframework -I../
LIBS=-lslog -lnetframework -lpthread -lcrypto #-lssl
FLAGS=-g

all:lib sfs

lib:SFSFile.h SFSFile.cpp ../sha1.h ../SFSProtocolFamily.h ../SFSProtocolFamily.cpp
	g++ -c ${INCS} -lnetframework $^ ${FLAGS}
	ar rc libsfsfile.a *.o

sfs:SFSClient.cpp SFSFile.h SFSFile.cpp ../sha1.h ../SFSProtocolFamily.h ../SFSProtocolFamily.cpp
	g++ ${INCS}  $^ ${LIBS} ${FLAGS} -o $@

clean:
	rm *.o *.h.gch sfs
