INCS=-I/usr/include/easynet -I../
LIBS=-leasynet -lpthread -llog4cplus -lcrypto #-lssl
FLAGS=-g -D__LOG4CPLUS__

all:lib sfds

lib:SFDSFile.h SFDSFile.cpp ../sha1.h ../SFDSProtocolFactory.h ../SFDSProtocolFactory.cpp
	g++ -c ${INCS} -leasynet $^ ${FLAGS}
	ar rc libsfdsfile.a *.o

sfds:SFDSClient.cpp SFDSFile.h SFDSFile.cpp ../sha1.h ../SFDSProtocolFactory.h ../SFDSProtocolFactory.cpp
	g++ ${INCS}  $^ ${LIBS} ${FLAGS} -o $@

clean:
	rm *.a *.o *.h.gch sfds
