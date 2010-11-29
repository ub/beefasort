
FLAGS = -Wall -fexceptions -march=native -fomit-frame-pointer -O3 -std=c++0x -ltbbmalloc_proxy -ltbbmalloc -lboost_filesystem-mt -lboost_system-mt -lboost_program_options-mt -ltbb -lrt

all: beefsort beefasort

.PHONY: all

%: %.cpp
	g++-4.5 $(FLAGS) -o $@ $<

 
