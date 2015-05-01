all:
	gcc inotify.c -o log_collector
	javac -cp ".:libs/*" log_parser.java
	javac -cp ".:libs/*" brolog_parser.java
	javac -cp ".:libs/*" log_packet.java


clean:
	rm log_collector
	rm log_parser.class
	rm log_packet.class
	rm brolog_parser.class
