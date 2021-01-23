install:
	pip3 install --upgrade -e .

test:
	rm -rf tests/out
	rm -f tests/test.db
	DEBUG=1 python3 tests/test_danql.py

.PHONY: test
