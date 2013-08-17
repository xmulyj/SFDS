INCLUDE=-I/usr/include/easynet -I../ 
LIBS=-leasynet -lpthread -llog4cplus
FLAGS=-g -D__LOG4CPLUS__
All:bin/chunk

bin/chunk:*.cpp *.h ../*.h ../*.cpp
	g++ ${INCLUDE} $^  ${LIBS} ${FLAGS} -o $@
clean:
	rm bin/chunk