//header files
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <signal.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <semaphore.h>

//definitions
#define MAX_SIZE 50
#define MAX_HISTORY 75
#define MAX_SUBMIT 25

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

//history struct used ot store the history of process executions
struct history_struct {
    int history_count,ncpu,tslice;
    sem_t mutex;
    struct Process history[MAX_HISTORY];
};

// struct for queue data structure
struct queue{
    int head,tail,capacity,curr;
    struct Process **table;
};

// struct for priority queue data structure
struct pqueue{
    int size,capacity;
    struct Process **heap;
};

//function declarations
void scheduler(int ncpu, int tslice);
static void my_handler(int signum);
void terminate();
void start_time(struct timeval *start);
unsigned long end_time(struct timeval *start);
bool queue_empty(struct queue *q);
int next_head(struct queue *q);
int next_tail(struct queue *q);
bool queue_full(struct queue *q);
void enqueue(struct queue *q, struct Process *proc);
void dequeue(struct queue *q);
bool pqueue_empty(struct pqueue *pq);
bool pqueue_full(struct pqueue *pq);
void swap(struct Process* a, struct Process* b);
void heapifyUp(struct pqueue* pq, int index);
void heapifyDown(struct pqueue* pq, int index); //min-heapify
void penqueue(struct pqueue *pq, struct Process *proc); //min-heap-insert
struct Process* pdequeue(struct pqueue *pq); //min-heap-extract-min

//global variables
int shm_fd;
bool term = false;
struct history_struct *process_table;
struct queue *running_q;
struct pqueue *ready_q;

int main(){
    //signal part to handle ctrl c (from lecture 7)
    struct sigaction sig;
    if (memset(&sig, 0, sizeof(sig)) == 0){
        perror("memset");
        exit(1);
    }
    sig.sa_handler = my_handler;
    if (sigaction(SIGINT, &sig, NULL) == -1){
        perror("sigaction");
        exit(1);
    }

    //accessing the shm in read-write mode
    shm_fd = shm_open("shm", O_RDWR, 0666);
    if (shm_fd == -1){
        perror("shm_open");
        exit(1);
    }
    process_table = mmap(NULL, sizeof(struct history_struct), PROT_READ|PROT_WRITE, MAP_SHARED, shm_fd,0);
    if (process_table == MAP_FAILED){
        perror("mmap");
        exit(1);
    }
    int ncpu = process_table->ncpu;
    int tslice = process_table->tslice;

    //initialising ready priority queue
    ready_q = (struct pqueue *) (malloc(sizeof(struct pqueue)));
    if (ready_q == NULL){
        perror("malloc");
        exit(1);
    }
    ready_q->size = 0;
    ready_q->capacity = MAX_SUBMIT;
    ready_q->heap = (struct Process **) malloc(ready_q->capacity * sizeof(struct Process));
    if (ready_q->heap == NULL){
        perror("malloc");
        exit(1);
    }
    for (int i=0; i<ready_q->capacity; i++){
        ready_q->heap[i] = (struct Process *)malloc(sizeof(struct Process));
        if (ready_q->heap[i] == NULL){
            perror("malloc");
            exit(1);
        }
    }
    //initialising running queue
    running_q = (struct queue *) (malloc(sizeof(struct queue)));
    if (running_q == NULL){
        perror("malloc");
        exit(1);
    }
    running_q->head = running_q->tail = running_q->curr = 0;
    running_q->capacity = ncpu+1;
    running_q->table = (struct Process **) malloc(running_q->capacity * sizeof(struct Process));
    if (running_q->table == NULL){
        perror("malloc");
        exit(1);
    }
    for (int i=0; i<running_q->capacity; i++){
        running_q->table[i] = (struct Process *)malloc(sizeof(struct Process));
        if (running_q->table[i] == NULL){
            perror("malloc");
            exit(1);
        }
    }

    // initialising a semaphore
    if (sem_init(&process_table->mutex, 1, 1) == -1){
        perror("sem_init");
        exit(1);
    }
    //creating daemon process
    if(daemon(1, 1)){
        perror("daemon");
        exit(1);
    }

    scheduler(ncpu, tslice);

    //cleanup for mallocs
    for (int i=running_q->capacity-1; i<0; i--) {
        free(running_q->table[i]);
    }
    free(running_q->table);
    free(running_q);
    for (int i=ready_q->capacity-1; i<0; i--){
        free(ready_q->heap[i]);
    }
    free(ready_q->heap);
    free(ready_q);
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
    return 0;
}

// scheduler function for scheduling and managing processes
void scheduler(int ncpu, int tslice){
    while(true){
        unsigned int remaining_sleep = sleep(tslice / 1000);
        if (remaining_sleep > 0){
            printf("Sleep was interrupted after %u seconds\n", remaining_sleep);
            exit(1);
        }
        if (sem_wait(&process_table->mutex) == -1){
            perror("sem_wait");
            exit(1);
        }
        //this if-block ensures that scheduler terminates after natural termination of all processes
        if (term && queue_empty(running_q) && pqueue_empty(ready_q)){
            terminate();
        }

        //adding process to ready queue if they have submit keyword
        for (int i=0; i<process_table->history_count; i++){
            if (process_table->history[i].submit==true && process_table->history[i].completed==false && process_table->history[i].queue==false){
                if (ready_q->size+ncpu < ready_q->capacity-1){
                    process_table->history[i].queue=true;
                    penqueue(ready_q, &process_table->history[i]);
                }
                else{
                    break;
                }
            }
        }

        //checking running queue and pausing the processes if they haven't terminated
        if (!queue_empty(running_q)){
            for (int i=0; i<ncpu; i++){
                if (!queue_empty(running_q)){
                    if (!running_q->table[running_q->head]->completed){
                        penqueue(ready_q, running_q->table[running_q->head]);
                        running_q->table[running_q->head]->execution_time += end_time(&running_q->table[running_q->head]->start);
                        running_q->table[running_q->head]->vruntime += running_q->table[running_q->head]->execution_time *running_q->table[running_q->head]->priority;
                        start_time(&running_q->table[running_q->head]->start);
                        if (kill(running_q->table[running_q->head]->pid, SIGSTOP) == -1){
                            perror("kill");
                            exit(1);
                        }
                        dequeue(running_q);
                    }
                    else{
                        dequeue(running_q);
                    }
                }
            }
        }

        //adding processes to running queue and resume their execution
        if (!pqueue_empty(ready_q)){
            for (int i=0; i<ncpu; i++){
                if (!pqueue_empty(ready_q)){
                    struct Process *proc = pdequeue(ready_q);
                    proc->wait_time += end_time(&proc->start);
                    start_time(&proc->start);
                    if (kill(proc->pid, SIGCONT) == -1){
                        perror("kill");
                        exit(1);
                    }
                    enqueue(running_q, proc);
                }
            }
        }
        if (sem_post(&process_table->mutex) == -1){
            perror("sem_post");
            exit(1);
        }
    }
}

//signal handler
static void my_handler(int signum){
    // handling SIGINT signal for termination
    if(signum == SIGINT){
        term = true;
    }
}

//function to terminate scheduler
void terminate(){
    printf("\nCaught SIGINT signal for termination\n");
    printf("Terminating simple scheduler...\n");
    //cleanups for malloc
    for (int i=running_q->capacity-1; i<0; i--) {
        free(running_q->table[i]);
    }
    free(running_q->table);
    free(running_q);
    for (int i=ready_q->capacity-1; i<0; i--){
        free(ready_q->heap[i]);
    }
    free(ready_q->heap);
    free(ready_q);
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
    exit(0);
}

//function to note start time
void start_time(struct timeval *start){
  gettimeofday(start, 0);
}

//function to get time duration since start time
unsigned long end_time(struct timeval *start){
  struct timeval end;
  unsigned long t;

  gettimeofday(&end, 0);
  t = ((end.tv_sec*1000000) + end.tv_usec) - ((start->tv_sec*1000000) + start->tv_usec);
  return t/1000;
}

//queue methods
bool queue_empty(struct queue *q){
    return q->head == q->tail;
}

int next_head(struct queue *q){
    if (q->head == q->capacity-1){
        return 0;
    }
    return q->head+1;
}

int next_tail(struct queue *q){
    if (q->tail == q->capacity-1){
        return 0;
    }
    return q->tail+1;
}

bool queue_full(struct queue *q){
    return next_tail(q) == q->head;
}

void enqueue(struct queue *q, struct Process *proc){
    if (queue_full(q)){
        printf("queue overflow, upper cap of 20 jobs at once\n");
        return;
    }
    q->curr++;
    q->table[q->tail] = proc;
    q->tail = next_tail(q);
}

void dequeue(struct queue *q){
    if (queue_empty(q)){
        printf("queue underflow\n");
        return;
    }
    q->curr--;
    q->head = next_head(q);
}

//pqueue methods
bool pqueue_empty(struct pqueue *pq){
    return pq->size == 0;
}

bool pqueue_full(struct pqueue *pq){
    return pq->size == pq->capacity;
}

void swap(struct Process* a, struct Process* b){
    struct Process temp = *a;
    *a = *b;
    *b = temp;
}

void heapifyUp(struct pqueue* pq, int index){
    while (index>0){
        int parent = (index-1)/2;
        if (pq->heap[index]->vruntime < pq->heap[parent]->vruntime){
            swap(pq->heap[index], pq->heap[parent]);
            index = parent;
        }
        else{
            break;
        }
    }
}

void heapifyDown(struct pqueue* pq, int index){
    int leftChild = 2*index + 1;
    int rightChild = 2*index + 2;
    int smallest = index;

    if (leftChild<pq->size && pq->heap[leftChild]->vruntime < pq->heap[smallest]->vruntime){
        smallest = leftChild;
    }

    if (rightChild<pq->size && pq->heap[rightChild]->vruntime < pq->heap[smallest]->vruntime){
        smallest = rightChild;
    }

    if (smallest != index){
        swap(pq->heap[index], pq->heap[smallest]);
        heapifyDown(pq, smallest);
    }
}

void penqueue(struct pqueue *pq, struct Process *proc){
    if (pq->size < pq->capacity){
        pq->heap[pq->size] = proc;
        heapifyUp(pq, pq->size);
        pq->size++;
    }
}

struct Process* pdequeue(struct pqueue *pq){
    if (pq->size>0){
        struct Process* removed = pq->heap[0];
        pq->heap[0] = pq->heap[pq->size - 1];
        pq->size--;
        heapifyDown(pq, 0);
        return removed;
    }
    return NULL;
}
