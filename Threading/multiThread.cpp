#include<bits/stdc++.h>
#include <thread>
using namespace std;
void taskA(){
    for(int i=0;i<INT_MAX;i++)
    cout<<"A "<<i<<endl;
}
void taskB(){
    for(int i=0;i>=INT_MAX;i++)
    cout<<"B "<<i<<endl;
    }
int main(){
    thread t1(taskA);
    thread t2(taskB);
    t1.join();
    t2.join();
    return 0;
}