#include <bits/stdc++.h>
using namespace std;
using namespace std::chrono;


long long int fib(int n){
    if (n==0) return 0;
    if (n==1) return 1;
    return fib(n-1) + fib(n-2);
}
long long int fibDP(int n){
    vector<long long int>dp(n + 1, 0);
    dp[0]=0;
    dp[1]=1;
    for(int i=2;i<=n;i++){
        dp[i] = dp[i-1] + dp[i-2];
    }
    return dp[n];
}
void printTime(const system_clock::time_point& tp) {
    auto duration = tp.time_since_epoch();
    auto millis = duration_cast<milliseconds>(duration).count() % 1000;

    time_t tt = system_clock::to_time_t(tp);
    tm local_tm = *localtime(&tt);

    cout << put_time(&local_tm, "%H:%M:%S") << ":" << setfill('0') << setw(3) << millis << endl;
}

int main(){
    int n;
    cout << "Enter the value of n: ";
    cin >> n;
    auto start = system_clock::now();
    cout << "Start time: ";
    printTime(start);

    long long result = fibDP(n);
    cout << "Fibonacci of " << n << " is: " << result << endl;

    auto end = system_clock::now();
    cout << "End time: ";
    printTime(end);

    auto duration = duration_cast<milliseconds>(end - start).count();
    cout << "Duration: " << duration << " ms" << endl;
    return 0;
}