#include <bits/stdc++.h>
using namespace std;
class Complex
{
    Complex()
    {
        cout << "\n new obj of Complex type is created";
    }

public:
    Complex(int a, int b)
    {
        this->a = a;
        this->b = b;
        cout << "\nconstructor with parameters called";
    }

private:
    int a, b;

public:
    void set_data(int a, int b)
    {
        this->a = a;
        this->b = b;
    }
    void show_data()
    {
        cout << "\n"
             << this->a << " + i" << this->b;
    }
    Complex add(Complex x)
    {
        Complex t;
        t.a = a + x.a;
        t.b = b + x.b;
        return t;
    }
};

int main()
{
    Complex x1(2, 3);
    Complex x2(4, 5);
    Complex x3 = x1.add(x2);
    x3.show_data();
    return 0;
}