#include <bits/stdc++.h>
using namespace std;
bool comparator(pair<int,int>&a, pair<int,int>&b){
    if(a.first<= b.first)return true;
    return false;
}
void dowhile(){
    int i=0;
    int x=0;
    do{
        if(i%5==0){
            cout<<x<<endl;
            x++;
        }
        ++i;
    }while(i<10);
    cout<<x<<endl;
 }
void pointers(){
    int a;
    int *p;
    a=10;
    p=&a;
    cout<<a<<endl;
    cout<<*p<<endl;
}
  
void addExp(){
    int j=10;
    int i;
    i=(j++,j+100,999+j);
    cout<<"addExp"<<endl;
    cout<<i<<endl;
    /*
    vectorPairSort.cpp:37:13: warning: left operand of comma operator has no effect [-Wunused-value]
    i=(j++,j+100,999+j);
           ~^~~~
    */

    cout<<"End of addExp"<<endl;
}
int main(){
    //create a vector of pairs with first element as int and second element as int and initialize it of length 5 and fill it with some values
    //clang++ -std=c++17 vectorPairSort.cpp -o vectorPairSort this is the command to compile the code with c++17 standard
    //clang++ -std=c++20 vectorPairSort.cpp -o vectorPairSort this is the command to compile the code with c++20 standard
    //vectorPairSort.cpp:9:24: error: a space is required between consecutive right angle brackets (use '> >') how to fix this error
    //creted a vector of pairs with first element as int and second element as int and fill it with some values avoiding the  above error
    enum{blue,green=5,red};
    cout<<blue<<red<<endl;
    dowhile();
    pointers();
    stringExp();
    addExp();
    vector<pair<int, int > > v;
    //push randon values to the vector
    v.push_back({1,2});
    v.push_back({2,3});
    v.push_back({2,2});
    v.push_back({7,1});
    v.push_back({4,9});
    //print the size of the vector
    cout<<v.size()<<endl;
    // vector<pair<int,int>>v= {{1,2},{5,3},{2,2},{7,1},{4,9}};
    sort(v.begin(),v.end());
    //print the sorted vector
    for(auto it :v){
        cout<<it.first<<" "<<it.second<<endl;
    }
    // for(auto it :v){
    //     cout<<it.first<<" "<<it.second<<endl;
    // }
    return 0;
}
