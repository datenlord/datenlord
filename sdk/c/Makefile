RPATH=$(PWD)/target/debug
OBJ_DIR=./build
DOC_DIR=./docs

CCFLAGS=-I./include
CXXFLAGS=-I./include -std=c++14
LDFLAGS=-L$(RPATH) -Wl,-rpath,$(RPATH)

LIBS=-ldatenlord -lgtest -lpthread

.PHONY: all
all: build test examples

.PHONY: format
format:
	cargo fmt
	find . -name '*.cpp' -exec clang-format -i --style=WebKit --verbose {} \;
	find . -name '*.c' -exec clang-format -i --style=WebKit --verbose {} \;

.PHONY: build
build:
	mkdir -p $(OBJ_DIR)
	cargo build

# build examples begin
EXAMPLES=$(wildcard ./examples/*.c)
EXAMPLE_OBJECTS=$(EXAMPLES:.c=.o)
EXAMPLE_TARGETS=$(EXAMPLES:.c=)
.PHONY: examples
examples: $(EXAMPLE_TARGETS)

$(EXAMPLE_TARGETS): % : %.o
	$(CC) $(CCFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

%.o: %.c
	$(CC) $(CCFLAGS) -c $< -o $@
# build examples end

.PHONY: clean
clean:
	cargo clean
	rm -rf $(EXAMPLE_OBJECTS)
	rm -rf $(EXAMPLE_TARGETS)
	rm -rf $(OBJ_DIR)
	rm -rf $(DOC_DIR)

