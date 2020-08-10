from rework import api

@api.task(domain='production')
def foo(task):
    pass
