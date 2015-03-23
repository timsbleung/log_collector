#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <limits.h>
#include <string.h>
 
#define MAX_EVENTS 1024 /*Max. number of events to process at one go*/
#define LEN_NAME 16 /*Assuming that the length of the filename won't exceed 16 bytes*/
#define EVENT_SIZE  ( sizeof (struct inotify_event) ) /*size of one event*/
#define BUF_LEN     ( MAX_EVENTS * ( EVENT_SIZE + LEN_NAME )) /*buffer to store the data of events*/

#define MAX_STRNLEN 4096

void generate_shell(char* name, char* dir) {
    if (is_valid_log_file(name)) {

        char * template = "#! /bin/bash\njava -classpath \"libs/*:.\" log_parser ";
        int len = strnlen(name, MAX_STRNLEN)+strnlen(template, MAX_STRNLEN)+1+strnlen(dir, MAX_STRNLEN);
        char * script = malloc(len);
        strcpy(script, template);
        strcat(script, name);
        strcat(script, " ");
        strcat(script, dir);

        FILE * fp = fopen("out/script.sh", "wb");
        if (fp!=NULL) {
            fputs(script, fp);
            fclose(fp);
            system("out/script.sh");
        }

        free(script);
        //fclose(fp); //causes problems
        //remove("out/script.sh"); //causes problems

    }
}

int is_valid_log_file(char* name) {
    return (string_ends_with(name, ".log") || string_ends_with(name, ".log.gz"));
}


 int string_ends_with(char * str, char * substr) {
    return (strlen(str) > 4 && !strcmp(str + strlen(str) - strlen(substr), substr));
}
 
int main( int argc, char **argv )
{
  int length, i = 0, wd;
  int fd;
  char buffer[BUF_LEN];

  char* watch_dir = argv[1];
 
  /* Initialize Inotify*/
  fd = inotify_init();
  if ( fd < 0 ) {
    perror( "Couldn't initialize inotify");
  }
 
  /* add watch to starting directory */
  wd = inotify_add_watch(fd, argv[1], IN_CREATE | IN_MODIFY | IN_DELETE | IN_CLOSE_WRITE);
 
  if (wd == -1)
    {
      printf("Couldn't add watch to %s\n",argv[1]);
    }
  else
    {
      printf("Watching:: %s\n",argv[1]);
    }
 
  /* do it forever*/
  while(1)
    {
      i = 0;
      length = read( fd, buffer, BUF_LEN ); 
 
      if ( length < 0 ) {
        perror( "read" );
      } 
 
      while ( i < length ) {
        struct inotify_event *event = ( struct inotify_event * ) &buffer[ i ];
        if ( event->len ) {
          if ( event->mask & IN_CLOSE_WRITE) {
              printf( "The file %s was modified CLOSE WRITE with WD %d\n", event->name, event->wd );      
              generate_shell(event->name, watch_dir);
          }
           
          if ( event->mask & IN_DELETE) {
            if (event->mask & IN_ISDIR)
              printf( "The directory %s was deleted.\n", event->name );      
            else
              printf( "The file %s was deleted with WD %d\n", event->name, event->wd );      
          } 
          i += EVENT_SIZE + event->len;
        }
      }
    }
 
  /* Clean up*/
  inotify_rm_watch( fd, wd );
  close( fd );
   
  return 0;
}


