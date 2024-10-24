.PHONY: install clean

install:
	pip install .

clean:
	find . -name "*.egg-info" -exec rm -rf {} +
