#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <sys/time.h> // execution time handling
#include <signal.h>  // signal handling
#include <errno.h>
#include <time.h> 
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>

#define MAX_LEN_CMD 1024
#define MAX_ARGS 100
#define MAX_PIPES 10
#define MAX_HISTORY 100
#define MAX_SIZE 50
#define MAX_WORDS 10
#define MAX_COMMANDS 5

//struct to store process info
struct Process{
    int pid, priority;
    bool submit,queue,completed; // flags
    // submit: process have been submitted
    // queue: process is in the scheduler's queue
    // completed: indicates if process have been completed
    char command[MAX_SIZE + 1]; //+1 to accomodate \n or \0
    struct timeval start;
    unsigned long execution_time, wait_time, vruntime;
};

//history struct used to store the history of process executions
struct history_struct {
    int history_count,ncpu,tslice;
    sem_t mutex; // semaphore
    struct Process history[MAX_HISTORY];
};

//function declarations
static void sigint_handler(int signum);
static void sigchld_handler(int signum, siginfo_t *info, void *context);
void terminateShell();
void shell_loop();
char* read_user_input();
int launch(char* command);
int createProcessAndRun(char* command);
int createChildProcess(char *command, int input_fd, int output_fd);
void startTime(struct timeval *start);
unsigned long endTime(struct timeval *start);
int submit_process(char *command);

//global variables
int shm_fd, scheduler_pid;
struct history_struct *process_table;

int main(int argc, char** argv){
    if (argc != 3){
        printf("Usage: %s <NCPU> <TIME_QUANTUM>\n",argv[0]);
        exit(1);
    }
    // shared memory initialisation
    // creating a new shared memory object using "shm_open"
    shm_fd = shm_open("shm", O_CREAT|O_RDWR, 0666);
    if (shm_fd == -1){
        perror("shm_open");
        exit(1);
    }
    // set desired size for shared memory segment using "ftruncate"
    if (ftruncate(shm_fd, sizeof(struct history_struct)) == -1){
        perror("ftruncate");
        exit(1);
    }
    // map the shared memory into process's address space using "mmap"
    process_table = mmap(NULL, sizeof(struct history_struct), PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd,0);
    if (process_table == MAP_FAILED){
        perror("mmap");
        exit(1);
    }

    process_table->history_count=0;
    process_table->ncpu = atoi(argv[1]);
    if (process_table->ncpu == 0){
        printf("invalid argument for number of CPU\n");
        exit(1);
    }
    process_table->tslice = atoi(argv[2]);
    if (process_table->tslice == 0){
        printf("invalid argument for time quantum\n");
        exit(1);
    }
    // initialising a semaphore
    if (sem_init(&process_table->mutex, 1, 1) == -1){  
        perror("sem_init");
        exit(1);
    }

    printf("Initializing simple scheduler...\n");
    // forking a child process for the scheduler
    pid_t pid;
    if ((pid= fork())<0){
        printf("fork() failed.\n");
        perror("fork");
        exit(1);
    }
    if (pid == 0){
        if (execvp("./scheduler",("./scheduler",NULL)) == -1) {
            printf("Couldn't initiate scheduler.\n");
            exit(1);
        }
        if (munmap(process_table, sizeof(struct history_struct)) < 0){
            printf("Error unmapping\n");
            perror("munmap");
            exit(1);
        }
        if (close(shm_fd) == -1){
            perror("close");
            exit(1);
        }
        exit(0);
    }
    else{
        scheduler_pid = pid;
    }

    //signal handling
    //sigint handler
    struct sigaction s_int, s_chld;
    if (memset(&s_int, 0, sizeof(s_int)) == 0){
        perror("memset");
        exit(1);
    }
    s_int.sa_handler = sigint_handler;
    if (sigaction(SIGINT, &s_int, NULL) == -1){
        perror("sigaction");
        exit(1);
    }

    if (memset(&s_chld, 0, sizeof(s_chld)) == 0){
        perror("memset");
        exit(1);
    }
    //sigchld handler
    s_chld.sa_sigaction = sigchld_handler;
    s_chld.sa_flags = SA_SIGINFO|SA_NOCLDSTOP|SA_RESTART;
    if (sigaction(SIGCHLD, &s_chld, NULL) == -1){
        perror("sigaction");
        exit(1);
    }

    printf("Initializing simple shell...\n");
    shell_loop();
    printf("Exiting simple shell...\n");

    terminateShell();
    // destroying the semaphore
    if (sem_destroy(&process_table->mutex) == -1){
        perror("shm_destroy");
        exit(1);
    }
    // unmapping shared memory segment followed by a "close" call
    if (munmap(process_table, sizeof(struct history_struct)) < 0){
        printf("Error unmapping\n");
        perror("munmap");
        exit(1);
    }
    if (close(shm_fd) == -1){
        perror("close");
        exit(1);
    }
    // parent deletes the shared memory object by using "shm_unlink"
    if (shm_unlink("shm") == -1){
        perror("shm_unlink");
        exit(1);
    }
    return 0;
}

//sigint handler
static void sigint_handler(int signum) {
    if(signum == SIGINT) {
        printf("\nCaught SIGINT signal for termination\n");
        printf("Terminating simple scheduler...\n");
        //send sigint to scheduler
        if (kill(scheduler_pid, SIGINT) == -1){
            perror("kill");
            exit(1);
        }
        // clean up and program termination
        printf("Exiting simple shell...\n");
        terminateShell();

        if (sem_destroy(&process_table->mutex) == -1){
            perror("shm_destroy");
            exit(1);
        }
        if (munmap(process_table, sizeof(struct history_struct)) < 0){
            printf("Error unmapping\n");
            perror("munmap");
            exit(1);
        }
        if (close(shm_fd) == -1){
            perror("close");
            exit(1);
        }
        if (shm_unlink("shm") == -1){
            perror("shm_unlink");
            exit(1);
        }
        exit(0);
    }
}

// This is a signal handler for SIGCHLD. It handles the termination of child processes, 
// updates information in the history table, and synchronizes access using a semaphore.
static void sigchld_handler(int signum, siginfo_t *info, void *context){
    if(signum == SIGCHLD){
        pid_t sender_pid = info->si_pid;
        if (sender_pid != scheduler_pid){
            if (sem_wait(&process_table->mutex) == -1){
                perror("sem_wait");
                exit(1);
            }
            for (int i=0; i<process_table->history_count; i++){
                if (process_table->history[i].pid == sender_pid){
                    process_table->history[i].execution_time += endTime(&process_table->history[i].start);
                    process_table->history[i].completed = true;
                    break;
                }
            }
            if (sem_post(&process_table->mutex) == -1){
                perror("sem_post");
                exit(1);
            }
        }
    }
}

//the function called upon termination to print command details
//in here we are formatting time and printing iterating over the global array
void terminateShell(){
    if (sem_wait(&process_table->mutex) == -1){
        perror("sem_wait");
        exit(1);
    }
    if (process_table->history_count > 0){
        //PID is -1 if a command was not executed through process creation
        printf("-----------------------------------------------\n");
        printf("Shell terminated. Scheduler terminated.\nCommand history details:\n");
        printf("-----------------------------------------------\n");
        //printf("\nCommand\t\tPID\t\tExecution_time\t\tWaiting_time\n");
        for (int i=0; i<process_table->history_count; i++){
            printf("S.No.-%d\n",i+1);
            printf("Command: %s\n",process_table->history[i].command);
            printf("PID: %d\n",process_table->history[i].pid);
            printf("Execution Time: %ldms\n",process_table->history[i].execution_time);
            printf("Waiting Time: %ldms\n", process_table->history[i].wait_time);
            printf("-----------------------------------------------\n");
            
        }
    }
    if (sem_post(&process_table->mutex) == -1){
        perror("sem_post");
        exit(1);
    }
}


//infinite loop for the shell
//we take user input, pass it over to launch and wait for execution and update time fields
void shell_loop(){
    int status;
    do{
        //this prints the output in magenta colour
        printf("AVShell:~$ "); //command prompt
        char* command = read_user_input();
        if (sem_wait(&process_table->mutex) == -1){
            perror("sem_wait");
            exit(1);
        }
        process_table->history[process_table->history_count].pid = -1;
        process_table->history[process_table->history_count].submit = false;
        process_table->history[process_table->history_count].wait_time = process_table->history[process_table->history_count].execution_time = process_table->history[process_table->history_count].vruntime = 0;
        startTime(&process_table->history[process_table->history_count].start);
        if (sem_post(&process_table->mutex) == -1){
            perror("sem_post");
            exit(1);
        }

        status = launch(command);
        if (sem_wait(&process_table->mutex) == -1){
            perror("sem_wait");
            exit(1);
        }
        if(!process_table->history[process_table->history_count].submit){
            process_table->history[process_table->history_count].execution_time = endTime(&process_table->history[process_table->history_count].start);
        }
        process_table->history_count++;
        if (sem_post(&process_table->mutex) == -1){
            perror("sem_post");
            exit(1);
        }
    } while(status);
}

//here we take input and remove trailing \n and update global array
char* read_user_input(){
    static char input[MAX_SIZE+1];
    if (fgets(input,MAX_SIZE+1,stdin) == NULL){
        perror("fgets");
        exit(1);
    }
    int input_len = strlen(input);
    if (input_len>0 && input[input_len-1]=='\n'){
        input[input_len-1] = '\0';
    }
    if (sem_wait(&process_table->mutex) == -1){
        perror("sem_wait");
        exit(1);
    }
    strcpy(process_table->history[process_table->history_count].command,input);
    if (sem_post(&process_table->mutex) == -1){
        perror("sem_post");
        exit(1);
    }
    return input;
}

//here we execute custom commands which dont require process creation and their pids are -1
int launch(char* command){
    //removing leading whitespaces
    while(*command==' ' || *command=='\t'){
        command++;
    }
    //removing trailing whitespaces
    char *end = command + strlen(command)-1;
    while (end>=command && (*end==' ' || *end=='\t')){
        *end = '\0';
        end--;
    }

    if (strncmp(command, "submit", 6) == 0) {
        // Check if the priority is specified
        if (sem_wait(&process_table->mutex) == -1){
            perror("sem_wait");
            exit(1);
        }
        process_table->history[process_table->history_count].submit = true;
        process_table->history[process_table->history_count].completed = false;
        process_table->history[process_table->history_count].priority = 1;
        process_table->history[process_table->history_count].queue = false;
        process_table->history[process_table->history_count].pid = submit_process(command);
        startTime(&process_table->history[process_table->history_count].start);
        if (sem_post(&process_table->mutex) == -1){
            perror("sem_post");
            exit(1);
        }
        return 1;
    }

    if (strcmp(command,"history") == 0){
        if (sem_wait(&process_table->mutex) == -1){
            perror("sem_wait");
            exit(1);
        }
        terminateShell();
        // for (int i=0; i<process_table->history_count+1; i++){
        //     printf("%s\n",process_table->history[i].command);
        // }
        if (sem_post(&process_table->mutex) == -1){
            perror("sem_post");
            exit(1);
        }
        return 1;
    }

    if (strcmp(command,"jobs") == 0){
        if (sem_wait(&process_table->mutex) == -1){
            perror("sem_wait");
            exit(1);
        }
        for (int i=0; i<process_table->history_count; i++){
            if (process_table->history[i].submit==true && process_table->history[i].completed==false){
                printf("%d\t%d\t%s\n",process_table->history[i].pid,process_table->history[i].priority,process_table->history[i].command);
            }
        }
        if (sem_post(&process_table->mutex) == -1){
            perror("sem_post");
            exit(1);
        }
        return 1;
    }

    if (strcmp(command, "") == 0){
        process_table->history_count--;
        return 1;
    }
    if (strcmp(command,"exit") == 0){
        terminateShell();
        return 0;
    }

    int status;
    status = createProcessAndRun(command);
    return status;
}

//here we check for the pipe and & in the commands and create child process after that in other function
int createProcessAndRun(char* command){
    //separating pipe commands (|)
    int command_count = 0;
    char* commands[MAX_COMMANDS];
    char* token = strtok(command, "|");
    while (token != NULL){
        commands[command_count++] = token;
        token = strtok(NULL, "|");
    }
    if (command_count>MAX_COMMANDS){
        printf("you have used more than 4 pipes, try again");
        return 1;
    }

    //executing if pipe is present in command input except the last one 
    int i, prev_read = STDIN_FILENO;
    int pipes[2], child_pids[command_count];
    //we iterate and execute every command through process creation and keep updating read and write ends of pipe
    for (i=0; i < command_count-1; i++){
        if (pipe(pipes) == -1){
            perror("pipe");
            exit(1);
        }

        if ((child_pids[i]=createChildProcess(commands[i], prev_read, pipes[1])) < 0){
            perror("createChildProcess");
            exit(1);
        }

        if (close(pipes[1]) == -1){
            perror("close");
            exit(1);
        }
        prev_read = pipes[0];
    }

    //the last command whose output is to be displayed on STDOUT
    //checking if it a background process
    bool background_process = 0;
    if (commands[i][strlen(commands[i]) - 1] == '&') {
        // Remove the '&' symbol
        commands[i][strlen(commands[i]) - 1] = '\0';
        background_process = 1;
    }
    if ((child_pids[i]=createChildProcess(commands[i], prev_read, STDOUT_FILENO)) < 0){
        perror("createChildProcess");
        exit(1);
    }
    
    //updating global array for pids
    if (sem_wait(&process_table->mutex) == -1){
        perror("sem_wait");
        exit(1);
    }
    process_table->history[process_table->history_count].pid = child_pids[i];
    if (sem_post(&process_table->mutex) == -1){
        perror("sem_post");
        exit(1);
    }
    if (!background_process) {
        //wait for child process if command is not background
        for (i = 0; i < command_count; i++) {
            int ret;
            int pid = waitpid(child_pids[i], &ret, 0);
            if (pid < 0) {
                perror("waitpid");
                exit(1);
            }
            if (!WIFEXITED(ret)){
                printf("Abnormal termination of %d\n", pid);
            }
        }
    }
    else{
        //print pid and command if it is being executed in background
        printf("%d %s\n", child_pids[command_count-1],command);
    }
}

int createChildProcess(char *command, int input_fd, int output_fd){
    int status = fork();
    if (status < 0){
        printf("fork() failed.\n");
        exit(1);
    }
    else if (status == 0){
        //child process
        //updating/copying I/O descriptors
        if (input_fd != STDIN_FILENO)
        {
            if (dup2(input_fd, STDIN_FILENO) == -1){
                perror("dup2");
                exit(1);
            }
            if (close(input_fd) == -1){
                perror("close");
                exit(1);
            }
        }
        if (output_fd != STDOUT_FILENO)
        {
            if (dup2(output_fd, STDOUT_FILENO) == -1){
                perror("dup2");
                exit(1);
            }
            if (close(output_fd) == -1){
                perror("close");
                exit(1);
            }
        }

        //creating an array of indiviudal command and its arguments
        char* arguments[MAX_WORDS+1]; //+1 to accomodate NULL
        int argument_count = 0;
        char* token = strtok(command, " ");
        while (token != NULL){
            arguments[argument_count++] = token;
            token = strtok(NULL, " ");
        }
        arguments[argument_count] = NULL;

        //exec to execute command (actual part of child process)
        if (execvp(arguments[0],arguments) == -1) {
            perror("execvp");
            printf("Not a valid/supported command.\n");
            exit(1);
        }
        exit(0);
    }
    else{
        //parent process
        return status;
    }
}

void startTime(struct timeval *start){
    gettimeofday(start, 0);
}

unsigned long endTime(struct timeval *start){
    struct timeval end;
    unsigned long t;

    gettimeofday(&end, 0);
    t = ((end.tv_sec*1000000) + end.tv_usec) - ((start->tv_sec*1000000) + start->tv_usec);
    return t/1000;
}

int submit_process(char *command){
    int priority, status;
    //creating an array of indiviudal command and its arguments
    char* arguments[MAX_WORDS+1]; //+1 to accomodate NULL
    int argument_count = 0;
    char* token = strtok(command, " "); //remove submit keyword from command
    token = strtok(NULL, " ");
    while (token != NULL){
        arguments[argument_count++] = token;
        token = strtok(NULL, " ");
    }
    //checking if priority is specified
    if (argument_count > 1){
        priority = atoi(arguments[--argument_count]);
        if (priority<1 || priority>4){
            printf("either invalid priority or you are passing arguments for a job");
            process_table->history[process_table->history_count].completed = true;
            return -1;
        }
        process_table->history[process_table->history_count].priority = priority;
    }
    arguments[argument_count] = NULL;

    status = fork();
    if (status < 0){
        printf("fork() failed.\n");
        exit(1);
    }
    else if (status == 0){
        //exec to execute command (actual part of child process)
        if (execvp(arguments[0],arguments) == -1) {
            perror("execvp");
            printf("Not a valid/supported command.\n");
            exit(1);
        }
        exit(0);
    }
    else{
        //parent process returns pid and stops child
        if (kill(status, SIGSTOP) == -1){
            perror("kill");
            exit(1);
        }
        return status;
    }
}
