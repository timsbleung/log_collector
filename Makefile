all:
	gcc inotify.c -o log_collector
	javac -cp ".:libs/*" log_parser.java
	javac brolog_parser.java


clean:
	rm log_collector
	rm log_parser.class
	rm brolog_parser.class
