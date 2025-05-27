#include <iostream>
#include<string>
using namespace std;
class Computer {   //product class
    string cpu_;
    string ram_;
    string storage_;
    public:
        void setCPU(const string & cpu){
            cpu_=cpu;
        }
        void setRAM(const string &ram){
            ram_=ram;
        }
        void setSTORAGE(const string &storage){
            storage_=storage;
        }
        void display()const{
                cout << "Computer Configuration:"
                  << "\nCPU: " << cpu_
                  << "\nRAM: " << ram_
                  << "\nStorage: " << storage_ << "\n\n";
        }
        
};
class Builder{   //builder interface
    public :
    virtual void buildCPU()=0;
    virtual void buildRAM()=0;
    virtual void buildSTORAGE()=0;
    virtual Computer getResult()=0;

};
class GamingComputerBuilder:public Builder{   //concrete builder
    Computer computer;
    public:
    void buildCPU (){
        computer.setCPU("Gaming CPU");
    }
    void buildRAM(){
        computer.setRAM("16GB");
    }
    void buildSTORAGE(){
        computer.setSTORAGE("1TB SSD");
    }
    Computer getResult(){
        return computer;
    }
};
class ComputerDirector{//director //optional
    public: 
    void construct(Builder &builder){
        builder.buildCPU();
        builder.buildRAM();
        builder.buildSTORAGE();
    }
};
int main(){
    GamingComputerBuilder gamingBuilderOBJ;
    ComputerDirector director;
    director.construct(gamingBuilderOBJ);
    Computer gamingComputer = gamingBuilderOBJ.getResult();
    gamingComputer.display();
    return 0;
}
