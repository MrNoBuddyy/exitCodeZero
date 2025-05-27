#include<bits/stdc++.h>
using namespace std;

class BTree{
    public:
    int data;
    BTree* left;
    BTree* right;
    BTree(int data){
        this->data = data;
        left = nullptr;
        right = nullptr;
    }
    void preOrder(BTree* root){
        if(root == nullptr) return;
        cout<<root->data<<" ";
        preOrder(root->left);
        preOrder(root->right);
    }
};


int main(){
    BTree* root=new BTree(1);
    root->left = new BTree(2);
    root->right = new BTree(3);
    root->left->left = new BTree(4);
    root->left->right = new BTree(5);
    root->right->left = new BTree(6);
    root->right->right = new BTree(7);
    root->preOrder(root);
    cout<<endl;

    return 0;

}