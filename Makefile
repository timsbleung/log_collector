all:
	gcc inotify.c -o log_collector
	javac -cp kafka_2.9.1-0.8.2.1.jar:scala-library-2.9.1.jar:. log_parser.java
	javac brolog_parser.java


clean:
	rm log_collector
	rm log_parser.class
	rm brolog_parser.class
