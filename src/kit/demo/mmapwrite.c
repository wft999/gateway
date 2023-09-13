#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/ipc.h>
#include <sys/sem.h>

union semun
{
    int val;
    struct Semid_ds* buf;
    unsigned short* array;
    struct seminfo* _buf;
};

static int CommPV(int semid,int which,int op){
    struct sembuf _sf;
    _sf.sem_op = op;
    _sf.sem_num = which;
    _sf.sem_flg = 0;
    return semop(semid,&_sf,1);
}

int main(int argc, char *argv[]) {
	int logFd;
	struct stat st;
	const char* dbLogName = "//home//lighthouse//temp//mmap1";
	int mappingSize = 8*1024*10;
	if (stat(dbLogName, &st) == 0){
		logFd = open(dbLogName, O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
	}else{
		logFd = open(dbLogName, O_RDWR| O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);

		if (posix_fallocate(logFd, 0, mappingSize) != 0) {
			close(logFd);
			unlink(dbLogName);
			return -1;
		}
	}

	char* pMem = mmap(0, st.st_size, PROT_WRITE | PROT_READ, MAP_SHARED, logFd, 0);
	if (pMem == MAP_FAILED) {
		close(logFd);
		return -2;
	}
	close(logFd);

	key_t key = ftok(dbLogName, 1);
	int sem_id = semget(key,0,0);
	if(sem_id == -1){
		sem_id = semget(key,1,IPC_CREAT|IPC_EXCL | 0600);
		if(sem_id == -1){
			munmap(pMem, mappingSize);
			return -3;
		}
	}

	union semun arg;
	arg.val = 0;
	semctl(sem_id,0,SETVAL,arg);
	for(int i =1; i < 250; i++){
		pMem[0] = i;
		pMem[i] = rand()%250;
		CommPV(sem_id,0,1);
		printf("cur=%d\n",pMem[i]);
		sleep(10);
	}

	munmap(pMem, mappingSize);
	return 0;
}
