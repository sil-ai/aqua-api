__version__ = 'v1'

import re
from dotenv import load_dotenv
load_dotenv('.env.pytest')
import app
import pytest
import inspect

def get_app_code():
    return list(filter(lambda row:row,inspect.getsource(app).splitlines()))

@pytest.fixture
def imports():
    app_code = get_app_code()
    return list(filter(lambda row:'from' in row, app_code))

def get_latest_routers():
    #all code in app.py as a list and cleaned of blanks
    app_code = get_app_code()
    latest = list(filter(lambda row:'prefix="/latest"' in row, app_code))
    base = list(filter(lambda row: re.search(r'^(?!.*prefix.*).*include_router\((.*)\)$',row), app_code))
    latest_routers = []
    for base_row, latest_row in zip(base,latest):
        base_router_name = re.search(r'include_router\((.*)\)',base_row).groups()[0]
        latest_router_name = re.search(r'include_router\((.*), prefix', latest_row).groups()[0]
        latest_routers.append((base_router_name, latest_router_name))
    return latest_routers

@pytest.mark.parametrize("base_router, latest_router",
                          get_latest_routers(),
                          ids=list(zip(*get_latest_routers()))[0]
)
def test_latest_routers(imports, base_router, latest_router):
    this_router_imports = list(filter(lambda item:base_router in item, imports))[0]
    #latest are equal to each other
    assert latest_router == base_router
    api_version = this_router_imports.split('.')[1]
    #latest router points down the correct path
    assert api_version in latest_router
    assert api_version in base_router
    #latest router version is highest
    router_name, version = re.search(r'(.*)_v(.*)',latest_router).groups()
    this_router = list(filter(lambda item:router_name in item, imports))
    latest_import_version = max([int(re.search(r'\.v(.*?)\.',item).groups()[0]) for item in this_router])
    assert latest_import_version == int(version)
