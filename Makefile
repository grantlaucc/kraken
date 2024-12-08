.PHONY: install clean

# Name of the virtual environment directory
VENV_DIR = env

install: $(VENV_DIR)/bin/activate
	$(VENV_DIR)/bin/pip install .

# Create the virtual environment if it doesn't exist
$(VENV_DIR)/bin/activate: 
	python3 -m venv $(VENV_DIR)
	$(VENV_DIR)/bin/pip install --upgrade pip

clean:
	find . -name "*.egg-info" -exec rm -rf {} +
