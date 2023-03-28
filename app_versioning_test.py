__version__ = 'v1'

import re
from dotenv import load_dotenv
load_dotenv('.env.pytest')
import app
import pytest
import importlib
import inspect

def get_app_code():
    app_code = list(filter(lambda row:row,inspect.getsource(app).splitlines()))
    #add in fake v2 imports
    imports = list(filter(lambda row:'from' in row, app_code))
    imports_v2 = [re.sub('1','2', item) for item in imports.copy()]
    #add in fake v2 routers
    latest = list(filter(lambda row:'prefix="/latest"' in row, app_code))
    latest_v2 = [re.sub('1','2',item) for item in latest]
    base = list(filter(lambda row: re.search(r'^(?!.*prefix.*).*include_router\((.*)\)$',row),app_code))
    base_v2 = [re.sub('1','2', item) for item in base]
    v1 = list(filter(lambda row: 'prefix="/v1"' in row, app_code))
    v2 = [re.sub('1','2',item) for item in v1.copy()]
    #put together the new app_code
    app_code = list(set(app_code) - set(latest) - set(base)) + latest_v2 + base_v2 + v2 + imports_v2
    return app_code

@pytest.fixture
def imports():
    app_code = get_app_code()
    imports = list(filter(lambda row:'from' in row, app_code))
    return imports

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

def get_first_routers():
    app_code = get_app_code()
    first_routers = list(filter(lambda row:'prefix="/v1"' in row, app_code))
    return [re.search(r'include_router\((.*), prefix',item).groups()[0] for item in first_routers]

def get_non_latest():
    latest_routers = [item[0] for item in get_latest_routers()]
    latest_versions = [tuple(['_'.join(item.split('_')[:2])] + item.split('_')[-1:]) for item in latest_routers]
    app_code = get_app_code()
    all_versioned_routers = list(filter(lambda item: re.search(r'prefix="/v',item), app_code))
    non_latest = []
    for router,version in latest_versions:
        non_latest.append(list(filter(lambda row: re.search(rf'(?=.*{router})(?!.*{version})',row), all_versioned_routers))[0])
    print(non_latest)
    return [re.search(r'include_router\((.*), prefix',item).groups()[0] for item in non_latest]

@pytest.mark.parametrize("base_router, latest_router",
                          get_latest_routers(),
                          ids=list(zip(*get_latest_routers()))[0]
)
def test_latest_routers(imports, base_router, latest_router):
    this_router_imports = list(filter(lambda item:base_router in item, imports))
    #latest are equal to each other
    assert latest_router == base_router
    api_version = this_router_imports[0].split('.')[1]
    #latest router points down the correct path
    assert api_version in latest_router
    assert api_version in base_router
    #latest router version is highest
    router_name, version = re.search(r'(.*)_v(.*)',latest_router).groups()
    this_router = list(filter(lambda item:router_name in item, imports))
    latest_import_version = max([int(re.search(r'\.v(.*?)\.',item).groups()[0]) for item in this_router])
    #should be the v2 that we injected earlier
    assert latest_import_version == int(version)

@pytest.mark.parametrize("first_router",
                          get_first_routers(),
                          ids=get_first_routers()
)
def test_router_paths(imports, first_router):
    this_router_imports = list(filter(lambda item:first_router in item, imports))[0]
    path = re.search(r'from (.*) import',this_router_imports).groups()[0]
    path_version = path.split('.')[1]
    route_import_command = f'import {path}'
    #get the route_file
    try:
        #??? How should I protect against injection attacks here?
        #should exist after the command
        exec(route_import_command)
        route_file = importlib.import_module(path)
        assert route_file.__version__ == path_version == first_router.split('_')[-1]
    except ModuleNotFoundError as err:
        raise AssertionError(err)

def test_older_routers():
    non_latest = get_non_latest()
    print(non_latest)
    #TODO: up to here
