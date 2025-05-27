#include <bits/stdc++.h>
using namespace std;

class genericTree {
public:
    int data;
    vector<genericTree*> children; // Store pointers to children

    genericTree(int data) {
        this->data = data;
    }

    void preOrder(genericTree* root) {
        if (root == nullptr) return;
        cout << root->data << " ";
        for (auto child : root->children) {
            preOrder(child); // Pass the pointer directly
        }
    }
};

int main() {
    genericTree* gtree = new genericTree(1);
    genericTree* child1 = new genericTree(2);
    genericTree* child2 = new genericTree(3);

    gtree->children.push_back(child1); // Store pointers, not objects
    gtree->children.push_back(child2);

    gtree->preOrder(gtree);
    cout << endl;

    // Clean up memory
    delete child1;
    delete child2;
    delete gtree;

    return 0;
}
