
/*** $ g++ -o iterativeInOrder iterativeInOrder.cpp
 $ ./iterativeInOrder
 
 g++ -std=c++11 -o iterativePostOrder iterativePostOrder.cpp 
 to avoid the auto warning of c++11
  */


#include <bits/stdc++.h>
using namespace std;
class Tree{
    public:
    int data;
    Tree *left;
    Tree *right;
    Tree(int data){
        this->data=data;
        this->left=NULL;
        this->right=NULL;
    }
};
void itrPreOrder(Tree*root){
    if(root==nullptr)return;
    stack<Tree*>st;
    st.push(root);
    while(st.size()){
        Tree*temp=st.top();
        st.pop();
        cout<<temp->data<<" ";
        if(temp->right)st.push(temp->right);
        if(temp->left)st.push(temp->left);
    }
    cout<<endl;
    return;
}
void itrInOrder(Tree*root){
    if(root==nullptr)return;
    stack<Tree*>st;
    Tree*temp=root;
    while (true){
        if(temp){
            st.push(temp);
            temp=temp->left;
        }else{
            if(st.empty())break;
            else{
               temp=st.top();
               st.pop();
               cout<<temp->data<<" ";
               temp=temp->right; 
            }
        }
        
    }
    cout<<endl;
    return;
}
void BFS(Tree*root){
    if(root==NULL)return;
    queue<Tree*>q;
    q.push(root);
    q.push(NULL);
    while(q.size()){
        Tree*temp=q.front();
        q.pop();
        if(!temp){
            cout<<endl;
            if(q.size()&& q.front()){
                q.push(NULL);
                continue;
            }else{
                break;
            }
        }else{
            cout<<temp->data<<" ";
            if(temp->left)q.push(temp->left);
            if(temp->right)q.push(temp->right);
        }
    }
    return;
}
void itrPostOrder(Tree*root){
    if(root==nullptr)return;
    vector<int>ans;
    stack<Tree*>st;
    st.push(root);
    while (st.size()){
        Tree*temp=st.top();
        st.pop();
        ans.push_back(temp->data);
        if(temp->left)st.push(temp->left);
        if(temp->right)st.push(temp->right);
    }
    for(auto it=ans.rbegin();it!=ans.rend();it++){
        cout<<*it<<" ";
    }
    
}
int main(){
    Tree* root=new Tree(5);
    root->left=new Tree(4);
    root->left->left=new Tree(2);
    root->left->right=new Tree(3);
    root->right=new Tree(7);
    root->right->right=new Tree(8);
    BFS(root);
    itrPreOrder(root);
    itrInOrder(root);
    itrPostOrder(root);
    return 1;
    cout<<"hello after return";
    return 0;
}