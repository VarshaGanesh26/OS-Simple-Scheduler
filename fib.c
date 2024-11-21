#include <stdio.h>
#include <stdlib.h>

int fib(int n) {
    if(n<2) return n;
    else return fib(n-1)+fib(n-2);
}

int main(){
    int val = fib(44);
    printf("%d\n", val);
    return 0;
}