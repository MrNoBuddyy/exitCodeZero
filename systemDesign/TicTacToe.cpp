#include<vector>
#include<exception>
// #include<bits/stdc++.h>
using namespace std;

class TicTacToe{
    int n;
    int **board;
    int * rowSum,*colSum;
    int diagSum=0,revDiagSum=0;
    public:
    TicTacToe(int n){
        this->n=n;
        board= new int *[n];
        for(int i=0;i<n;i++){
            board[i]=new int[n];
            for(int j=0;j<n;j++)board[i][j]=0;
        }
        rowSum=new int[n];
        colSum=new int [n];
        int diagSum;
        int revDiagSum;
    }
    ~TicTacToe(){
        delete []rowSum;
        delete[]colSum;
        for(int i=0;i<n;i++)delete[]board[i];
        delete[]board;
    }
    int move(int player,int row,int col){
        if(row<0 ||col<0 ||row>=n ||col>=n){
            throw invalid_argument("Out of boundry exception");
        }else if(board[row][col]!=0){
            throw invalid_argument("Square is alredy occupied");
        }else if(player!=0 ||player!=1){
            throw invalid_argument("Invalid player");
        }else{
            board[row][col]=player==0?-1:+1;
            player=player==0?-1:+1; 
            rowSum[row]+=player;
            colSum+=player;
            if(row==col)
                diagSum+=player;
            if(row=n-1-col)
                revDiagSum+=player;
            bool win =true;
            if(abs(rowSum[row])==n || abs(colSum[col])==n || abs(diagSum)==n || abs(revDiagSum)==n)return player;
            return 0;

        }

    }

};