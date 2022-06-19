.PHONY: test build
all: test build

test:
	python -m unittest

build:
	python -m build