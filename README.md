# Assignment 2

## Installation

### Virtual Environment

A virtual environment is useful for containing dependencies and packages to a specific project. This can be done by using the included Python standard library `venv`.

Create a virtual environment inside the root folder of the project:

```bash
# Unix/macOS
python3 -m venv env
```

```bash
# Windows
py -m venv env
```

Activate the virtual environment:

```bash
# Unix/macOS
source env/bin/activate
```

```bash
# Windows
.\env\Scripts\activate
```

Install the required packages inside the activated virtual environment:

```bash
pip install -r requirements.txt
```

Deactivate the virtual environment:

```bash
deactivate
```

### Environment Variables

A `.env` file is required to be located in the root folder of the project.

An overview of the required environment variables can be found in the example file `.env.example`, located in the root folder.

