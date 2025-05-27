#include <bits/stdc++.h>
using namespace std;
int main(){
    int n;
    cin>>n;
    vector<int>ans(n-1);
    for(int i=0;i<n-1;i++)cin>>ans[i];
    sort(ans.begin(),ans.end());
    for(int i=1;i<n;i++){
        if(ans[i-1]!=i){
            cout<<i;
            return 0;}
    }
    cout<<n;
    return 0;
}