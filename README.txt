pyenv virtualenv 3.12.0 venv-cronch
pyenv local venv-cronch
python -m pip install --upgrade pip
pip install pip-tools
pip-compile requirements.in
pip intall -r requirements.txt
