# NOTE: need to run setup jdk first, and run gmake

DIR = .
SRCS = $(wildcard $(DIR)/*/*.java)
CLASSES = $(wildcard $(DIR)/*/*.class)
OBJS = $(SRCS:.java=.class)

all:	$(OBJS)

run:
	java host.Host
leader:
	java host.Host 1
hosts:
	java host.Host 2

clean: FORCE
	rm -rf $(CLASSES)
	rm -rf ./host/*.class ./message/*.class ./leaderelection/*.class
	rm -rf 129.210.16.*

.SUFFIXES: .java .class

.java.class:
	make clean
	javac $(SRCS)

FORCE:
