#include <thread>
#include <iostream>


void workFunc(int* ptr, size_t times)
{
	while(times--) {
		*ptr += 1;
	}
}


int main(int argc, char const *argv[])
{
	int* numbers = new int[3];

	std::thread t1(workFunc, numbers, 500);
	std::thread t2(workFunc, numbers + 1, 600);
	std::thread t3(workFunc, numbers + 2, 700);


	t1.join();
	t2.join();
	t3.join();

	for(int i = 0; i < 3; ++i)
		std::cout << "Number Slot " << (int)i << " is " << (int)numbers[i] << std::endl;

	delete[] numbers;
	return 0;
}