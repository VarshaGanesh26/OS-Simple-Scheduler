default:
	gcc simpleShell.c -o shell -lpthread
	gcc simpleScheduler.c -o scheduler -lpthread

clean:
	-@rm -f scheduler shell