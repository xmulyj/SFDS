INCLUDE=-I/usr/include/easynet -I/usr/include/mysql++ -I/usr/include/mysql  -I../ 
LIBS=-leasynet -lpthread -llog4cplus -L/usr/lib/mysql/ -lmysqlclient -lmysqlpp
FLAGS=-g -D__LOG4CPLUS__

All:bin/master

bin/master:*.cpp *.h ../*.h ../*.cpp
	g++ ${INCLUDE} $^  ${LIBS} ${FLAGS} -o $@
clean:
	rm bin/master