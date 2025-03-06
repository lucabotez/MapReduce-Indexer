# Copyright @lucabotez

build:
		g++ map_reduce.cpp -o map_reduce -lpthread -Wall -Wextra -Werror -std=c++17
clean:
		rm map_reduce
