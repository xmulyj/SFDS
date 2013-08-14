INCLUDE=-I/usr/include/easynet -I/usr/include/mysql++ -I/usr/include/mysql  -I../ 
LIBS=-leasynet -lpthread -llog4cplus -L/usr/lib/mysql/ -lmysqlclient -lmysqlpp

All:bin/master

bin/master:*.cpp *.h ../*.h ../*.cpp
	g++ ${INCLUDE} $^  ${LIBS} -o $@
clean:
	rm bin/master