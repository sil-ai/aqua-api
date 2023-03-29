__version__ = 'v2'

import re
import importlib
import inspect
import pytest
from dotenv import load_dotenv
import app
load_dotenv('.env.pytest')

"""
Tests app versioning for correctness

"""

def get_app_code():
    """ Gets the app code from app.py """
    return list(filter(lambda row:row,inspect.getsource(app).splitlines()))

@pytest.fixture
def imports():
    """imports code fixture"""
    app_code = get_app_code()
    imports_code = list(filter(lambda row:'from' in row, app_code))
    return imports_code

def get_latest_routers():
    """ all code in app.py as a list and cleaned of blanks """
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
    """ returns v1 router variable names """
    app_code = get_app_code()
    first_routers = list(filter(lambda row:'prefix="/v1"' in row, app_code))
    return [re.search(r'include_router\((.*), prefix',item)\
                       .groups()[0] for item in first_routers]

def get_non_latest():
    """ gets router variables that are not the latest """
    latest_routers = [item[0] for item in get_latest_routers()]
    latest_versions = [tuple(['_'.join(item.split('_')[:2])]\
                      + item.split('_')[-1:]) for item in latest_routers]
    app_code = get_app_code()
    all_versioned_routers = list(filter(lambda item:\
                            re.search(r'prefix="/v',item), app_code))
    non_latest = []
    for router,version in latest_versions:
        filter_regex = f'(?=.*{router})(?!.*{version})'
        non_latest.append(list(filter(lambda row: re.search(filter_regex, row),\
                               all_versioned_routers))[0])
    print(non_latest)
    return [re.search(r'include_router\((.*), prefix',item)\
                     .groups()[0] for item in non_latest]

@pytest.mark.parametrize("base_router, latest_router",
                          get_latest_routers(),
                          ids=list(zip(*get_latest_routers()))[0]
)
def test_latest_routers(imports, base_router, latest_router):
    """ tests the latest routers """
    this_router_imports = list(filter(lambda item:latest_router in item, imports))
    #latest are equal to each other
    #TODO: add this back in when client is not on v1
    #assert latest_router == base_router
    api_version = this_router_imports[0].split('.')[1]
    #latest router points down the correct path
    assert api_version in latest_router
    #TODO: add this back in when client is not on v1
    #assert api_version in base_router
    #latest router version is highest
    router_name, version = re.search(r'(.*)_v(.*)',latest_router).groups()
    this_router = list(filter(lambda item:router_name in item, imports))
    latest_import_version = max([int(re.search(r'\.v(.*?)\.',item)\
                               .groups()[0]) for item in this_router])
    #should be the v2 that we injected earlier
    assert latest_import_version == int(version)

@pytest.mark.parametrize("first_router",
                          get_first_routers(),
                          ids=get_first_routers()
)
def test_router_paths(imports, first_router):
    """ tests that the import router paths go to the correct file """
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

@pytest.mark.parametrize(
    "non_latest",
    get_non_latest(),
    ids= get_non_latest()
)
def test_older_routers(imports, non_latest):
    """ tests routers before the latest one """
    this_router_imports = list(filter(lambda item:non_latest in item, imports))
    api_version = this_router_imports[0].split('.')[1]
    assert api_version in non_latest
