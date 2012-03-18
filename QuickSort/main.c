//
//  main.c
//  QuickSort
//
//  Created by Carl Wieland on 3/17/12.
//  Copyright (c) 2012 __MyCompanyName__. All rights reserved.
//

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

#define SIZE 6500000

double When();
int sort(const void* a, const void*x);
int getPivot(int* array, int size);



int main(int argc, char * argv[])
{
   
    int nproc, iproc;
    MPI_Status status;

    
    MPI_Init(&argc, &argv);

    int i = 0;
    double starttime;
    int* pivot=(int*)malloc(sizeof(int));
    int* send=(int*)malloc(sizeof(int));
    int* recv=(int*)malloc(sizeof(int));
    MPI_Comm_size(MPI_COMM_WORLD, &nproc);
    MPI_Comm_rank(MPI_COMM_WORLD, &iproc);
    int mySize=SIZE/nproc;
    
    //my values
    int* vals = (int*)malloc( SIZE * sizeof(int));
    //for holding their values that come in
    int* theirs = (int*)malloc( SIZE * sizeof(int));
    //for joining the two and
    int* tmp = (int*)malloc( SIZE * sizeof(int));
    
    
    for (i=0; i<mySize; i++) 
        vals[i]=rand();
    
    
    
    //sort our valus
    qsort(vals, mySize, sizeof(int), sort);
 
    
    
    
    MPI_Comm newcomm;
    MPI_Comm_split(MPI_COMM_WORLD, 1, iproc, &newcomm);    
    
   
    int groupSize=nproc;
    starttime = When();

    while (groupSize>1) {
        MPI_Comm_size(newcomm, &nproc);
        MPI_Comm_rank(newcomm, &iproc);        
        //Find the Pivot
        *pivot=getPivot(vals, mySize);
        
        if(0){
            //send it out among the group
            MPI_Bcast(pivot, 1, MPI_INT, 0, newcomm);
        }
        else{
            //median of all in 
            
        }
        //calculate how many we will send
        *send=0;
        for (i=0; i<mySize; i++) {
            if(iproc < nproc / 2){
                if (vals[i] >= *pivot) {
                    tmp[*send]=vals[i];
                    (*send)++;
                    
                }
            }
            else{
                if (vals[i] <= *pivot) {
                    tmp[*send]=vals[i];
                    (*send)++;
                    
                }
            }
        }
        
        //smalls send first
        if(iproc < nproc/2){
            
            //send the size then the values
            //fprintf(stderr,"\t\t\t%d: sending %d to %d\n", iproc, *send, iproc+(nproc/2));
            MPI_Send(send, 1, MPI_INT, iproc+(nproc/2), 0, newcomm);
            MPI_Send(tmp, *send, MPI_INT, iproc+(nproc/2), 0, newcomm);
            //we recieve the two
            MPI_Recv(recv, 1, MPI_INT, iproc+(nproc/2), 0, newcomm, &status);
            //fprintf(stderr,"\t\t\t\t%d: reciving %d from %d\n", iproc, *recv, iproc+(nproc/2));
            MPI_Recv(theirs, *recv, MPI_INT, iproc+(nproc/2), 0, newcomm, &status);
        }
        else {
            
            //we recieve the two
            MPI_Recv(recv, 1, MPI_INT, iproc-(nproc/2), 0, newcomm, &status);
            //fprintf(stderr,"\t\t\t\t%d: reciving %d from %d\n", iproc, *recv, iproc-(nproc/2));
            MPI_Recv(theirs, *recv, MPI_INT, iproc-(nproc/2), 0, newcomm, &status);
            
            //send the size then the values
            //fprintf(stderr,"\t\t\t%d: sending %d to %d\n", iproc, *send, iproc-(nproc/2));
            MPI_Send(send, 1, MPI_INT, iproc-(nproc/2), 0, newcomm);
            MPI_Send(tmp, *send, MPI_INT, iproc-(nproc/2), 0, newcomm);
        }
        //now we combine the theirs and what is left of ours.
        
        if(iproc<nproc/2){
            mySize-=*send;

            for (i=0; i<*recv; i++) {
                vals[mySize]=theirs[i];
                mySize++;
                
            }
        }
        else{
            int off=0;
            for (i=0; i<mySize; i++) {
                if(vals[i]>= *pivot){
                    theirs[*recv+off]=vals[i];
                    off++;
                }
            }
            int* temp=vals;
            vals=theirs;
            theirs=temp;
            mySize=*recv+(mySize-*send);
        }
        //fprintf(stderr,"%d:my size:%i,\n",iproc, mySize);
        qsort(vals, mySize, sizeof(int), sort);
        /*for (i=0; i<mySize; i++) {
            fprintf(stderr,"%d:%i,\t",iproc, vals[i]);
            
        }      
        fprintf(stderr,"\n");*/
        
        
        
        
        
        
        
        //reset the size of the group
        MPI_Comm_split(newcomm, iproc < nproc/2 , iproc, &newcomm);
        groupSize/=2;
    }
    
    MPI_Comm_rank(MPI_COMM_WORLD, &iproc);

    fprintf(stderr,"\n%d:done: %f elements: %i\n", iproc, (When()-starttime),mySize);
    free(vals);
    free(theirs);
    free(tmp);
    free(send);
    free(recv);
   
    MPI_Finalize();
   
    
    return 0;
}

int getPivot(int* array, int size){
    return array[rand()%size];
    //return array[size/2];
}

int sort(const void *x, const void *y) {
    return (*(int*)x - *(int*)y);
}


/* Return the correct time in seconds, using a double precision number.       */
double When()
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return ((double) tp.tv_sec + (double) tp.tv_usec * 1e-6);
}


