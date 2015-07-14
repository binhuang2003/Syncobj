INCLUDE = -I./inc/ -I../shared/inc -I../shared/inc/oracle/
LIBPATH = -L../shared/lib -L../shared/lib/instantclient_10_2/
LIBS = -lm -lnsl -lz -lmysqlclient -lclntsh -lnnz10
#CFLAGS = -g -Wall
CFLAGS = -O2 -Wall 

OBJS = comm.o cmpDir.o cmpSeq.o cmpSource.o cmpSyn.o cmpTable.o cmpView.o main.o

all: syncobj

syncobj: $(OBJS)
	g++ -m32 -o syncobj $(OBJS) $(LIBS) $(LIBPATH)
	@echo 'Finished building target: $@'
%.o: src/%.cpp
	g++ -m32 -o $@ $(INCLUDE) $(CFLAGS) -c $<

clean:
	-rm $(OBJS)
