#include <bits/stdc++.h>
using namespace std;

class AVLTree{
    public:
    AVLTree*left,*right;
    int height;
    int val;
};
    int nodeHeight(AVLTree* N){
        if(N==NULL)return 0;
        return N->height;
    }
    int max(int a,int b){
        return (a>b)?a:b;
    }
    AVLTree*newNode(int val){
        AVLTree*node=new AVLTree();
        node->val=val;
        node->left=NULL;
        node->right=NULL;
        node->height=1;
        return node;
    }
    AVLTree*rightRotate(AVLTree*y){
        AVLTree*x=y->left;
        AVLTree*xr=x->right;
        x->right=y;
        y->left=xr;
        y->height=max(nodeHeight(y->left),nodeHeight(y->right)+1);
        x->height=max(nodeHeight(x->left),nodeHeight(x->right)+1);
        return x;
    }
    AVLTree*leftRotate(AVLTree*x){
        AVLTree*y=x->right;
        AVLTree*yl=y->left;
        y->left=x;
        x->right=yl;
        x->height=max(nodeHeight(x->left),nodeHeight(x->right)+1);
        y->height=max(nodeHeight(y->left),nodeHeight(y->right)+1);
        return y;
    }
    int getBalance(AVLTree*n){
        if(n==NULL)return 0;
        return nodeHeight(n->left)-nodeHeight(n->right);
    }
    AVLTree * insert(AVLTree*root,int val){
        if(root==NULL) return newNode(val);
        if(val<root->val) {
            root->left=insert(root->left,val);
        }else if(val>root->val){
            root->right=insert(root->right,val);
        }
        else return root; //equal values are not allowd in avl tree
        root->height=1+max(nodeHeight(root->left),nodeHeight(root->right));
        int balance=getBalance(root);
        if(balance>1){
            if(val<root->left->val){
                return leftRotate(root);
            }else if(val>root->right->val){
                root->right=rightRotate(root->right);
                return leftRotate(root);
            }
        }
        return root;
    }
    AVLTree* minValNode(AVLTree * root){
        if(root==NULL)return NULL;
        AVLTree*current=root;
        if(current->left)current=current->left;
        return current;
    }
    AVLTree *deleteNode(AVLTree*root,int val){
        if(root==NULL)return root;
        if(val<root->val)root->left=deleteNode(root->left,val);
        else if(val>root->val)root->right=deleteNode(root->right,val);
        else{
            if(root->left==NULL || root->right==NULL){
                AVLTree*temp=root->left?root->left:root->right;
                if(temp==NULL){
                    temp=root;
                    root=NULL;
                }else *root=*temp;
                free(temp); 
            }else{
                AVLTree*temp=minValNode(root->right);
                root->val=temp->val;
                root->right=deleteNode(root->right,temp->val);
            }
        }
        if(root==NULL)return root;
        root->height=1+max(nodeHeight(root->left),nodeHeight(root->right));
        int balance=getBalance(root);
        if(balance>1){
            if(getBalance(root->left)>=0)return rightRotate(root);
            else {
                root->left=leftRotate(root->left);
                return rightRotate(root);
            }
        }
        return root;
    }
    void printTree(AVLTree*root,string indent,bool last){
        if(root!=nullptr){
            cout<<indent;
            if (last) {
                cout << "R----";
                indent += "   ";
            } else {
                cout << "L----";
                indent += "|  ";
            }
            cout << root->val << endl;
            printTree(root->left, indent, false);
            printTree(root->right, indent, true);
        }

    }
    int main() {
  AVLTree *root = NULL;
  root = insert(root, 33);
  root = insert(root, 13);
  root = insert(root, 53);
  root = insert(root, 9);
  root = insert(root, 21);
  root = insert(root, 61);
  root = insert(root, 8);
  root = insert(root, 11);
  printTree(root, "", true);
  root = deleteNode(root, 13);
  cout << "After deleting " << endl;
  printTree(root, "", true);
}