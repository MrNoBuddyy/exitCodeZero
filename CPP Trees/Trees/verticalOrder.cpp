
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
    cout<<"itrPreOrder"<<endl;
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
    cout<<"itrInOrder"<<endl;
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
    cout<<"BFS"<<endl;
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
    cout<<"itrPostOrder"<<endl;
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
    cout<<endl;
}
void verticalOrder(Tree*root){
    cout<<"verticalOrder"<<endl;
    if(root==nullptr)return;
    map<int,map<int,multiset<int> > >mp;
    queue<pair<Tree*,pair<int,int> > >st;
    int x=0,y=0;
    st.push({root,{x,y}});
    while (st.size()){
        auto pp=st.front();
        st.pop();
        Tree*temp=pp.first;
        x=pp.second.first;
        y=pp.second.second;
        mp[x][y].insert(temp->data);
        if(temp->left)st.push({temp->left,{x-1,y+1}});
        if(temp->right)st.push({temp->right,{x+1,y+1}});
    }
    for(auto itx:mp){
        for(auto ity:itx.second){
            for(auto mset:ity.second){
                cout<<mset<<" ";
            }
        }
        cout<<endl;
    }
    return;
}
void topView(Tree*root){
    cout<<"topView"<<endl;
    if(root==nullptr)return;
    queue<pair<Tree*,int>>q;
    map<int,int>mp;
    q.push({root,0});
    int x=0;
    Tree*temp=NULL;
    while (q.size()){
        auto p=q.front();
        q.pop();
        temp=p.first;
        x=p.second;
        auto it=mp.find(x);
        if(it==mp.end()){
            mp[x]=temp->data;
        }
        if(temp->left)q.push({temp->left,x-1});
        if(temp->right)q.push({temp->right,x+1});
    }
    for(auto it:mp)cout<<it.second<<" ";
    cout<<endl;
    return;
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
    verticalOrder(root);
    topView(root);
    return 1;
    cout<<"hello after return";
    return 0;
}